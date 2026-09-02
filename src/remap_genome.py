#!/usr/bin/env python
"""remap_genome.py: hybrid duckdb + python (polars-free, byte-identical).

duckdb (columnar, OOM-safe): reads parquet, maps transcript-aligned sites to
genome coords (strand-aware), unions with genome sites, and GROUP BY
(Chrom,Pos,Strand) -> MIN(Motif), summed counts, per-site (GeneIdx,GenePos)
lists, depth filter.
python: builds the ';'-joined GeneName/GenePos in gene-index order (the part
that is cleaner outside SQL), then writes the TSV(.gz) directly.
"""

import argparse
import csv
import gzip
import logging
import os
import re
import tempfile
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def parse_transcript(tx_file):
    order, seen, exons = [], set(), []
    raw = open(tx_file).read().splitlines()
    if not raw:
        return [], [], {}
    header = raw[0].split("\t")
    cols = {c: i for i, c in enumerate(header)}
    for need in ("gene_id", "chrom", "strand", "spans"):
        if need not in cols:
            raise ValueError("annotation header missing %r (has %s)"
                             % (need, header))
    gi, ci, ri, si = cols["gene_id"], cols["chrom"], cols["strand"], cols["spans"]
    for line in raw[1:]:
        parts = line.split("\t")
        if len(parts) <= si:
            continue
        gid = parts[gi]
        if gid not in seen:
            seen.add(gid)
            order.append(gid)
    for line in raw[1:]:
        parts = line.rstrip("\n").split("\t")
        if len(parts) <= si:
            continue
        gid, chrom, strand, spans = parts[gi], parts[ci], parts[ri], parts[si]
        exon_len = 0
        for span in spans.split(","):
            a, b = span.split("-", 1)
            g_start, g_end = int(a) - 1, int(b)
            length = g_end - g_start
            tx_start, tx_end = exon_len, exon_len + length
            exon_len += length
            exons.append((gid, chrom, strand, g_start, g_end, tx_start, tx_end))
    gmap = {g: i for i, g in enumerate(order)}
    return order, exons, gmap


def q(v):
    return "'" + str(v).replace("'", "''") + "'"


def _gzip_member(chunk: bytes, level: int) -> bytes:
    """One standalone gzip member (what pigz emits, concatenated)."""
    co = zlib.compressobj(level, zlib.DEFLATED, 31)  # 31 = gzip container
    return co.compress(chunk) + co.flush(zlib.Z_FINISH)


class ParallelGzipWriter:
    """Streaming gzip writer that compresses on a thread pool.

    Each fixed-size chunk becomes its own gzip member; the members are
    concatenated, which is exactly the pigz layout and is accepted by every
    gzip reader (decompresses to the identical byte stream). zlib releases the
    GIL while deflating, so threads scale with cores; members are written
    strictly in order. Roughly 9x faster than the single-threaded gzip module
    at the same level and size on the remap output.
    """

    def __init__(self, path, level=6, threads=None, chunk_bytes=4 << 20):
        self._fh = open(path, "wb")
        self._level = level
        self._chunk_bytes = chunk_bytes
        self._buf = bytearray()
        workers = max(1, min(threads or (os.cpu_count() or 4), 16))
        self._pool = ThreadPoolExecutor(max_workers=workers,
                                        thread_name_prefix="gz")
        self._pending = deque()

    def write(self, text):
        self._buf += text.encode()
        while len(self._buf) >= self._chunk_bytes:
            chunk = bytes(self._buf[:self._chunk_bytes])
            del self._buf[:self._chunk_bytes]
            self._pending.append(self._pool.submit(_gzip_member, chunk, self._level))
        # drain finished members (in order) without blocking on the head
        while self._pending and self._pending[0].done():
            self._fh.write(self._pending.popleft().result())

    def close(self):
        if self._buf:
            self._pending.append(self._pool.submit(
                _gzip_member, bytes(self._buf), self._level))
            self._buf.clear()
        while self._pending:  # flush the rest, in order
            self._fh.write(self._pending.popleft().result())
        self._pool.shutdown()
        self._fh.close()


def remap_and_join_files_parquet(gene_df_file, genome_df_file, transcript_file,
                                 output_file, min_depth=1):
    gene_order, exons, gidx = parse_transcript(transcript_file)
    con = duckdb.connect()
    # Bulk-load the exon map. con.executemany("INSERT ...", rows) is ~1 ms per
    # row in duckdb (~140 s for 150k exons!) - instead stage the rows in a temp
    # TSV and let duckdb's vectorized CSV reader load them in well under a second.
    with tempfile.NamedTemporaryFile(
        "w", suffix=".tsv", prefix="remap_txmap_", delete=False,
        dir=str(Path(output_file).parent),
    ) as _tf:
        _tf.write("GeneName\tChrom\tStrand\tg_start\tg_end\ttx_start\ttx_end\n")
        _tf.writelines("\t".join(map(str, e)) + "\n" for e in exons)
        _txmap_tsv = _tf.name
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE txmap AS
            SELECT c0 AS GeneName, c1 AS Chrom, c2 AS Strand,
                   CAST(c3 AS BIGINT) AS g_start,
                   CAST(c4 AS BIGINT) AS g_end,
                   CAST(c5 AS BIGINT) AS tx_start,
                   CAST(c6 AS BIGINT) AS tx_end
            FROM read_csv('{_txmap_tsv}', delim='\t', header=true,
                          columns={{'c0':'VARCHAR','c1':'VARCHAR','c2':'VARCHAR',
                                    'c3':'VARCHAR','c4':'VARCHAR','c5':'VARCHAR',
                                    'c6':'VARCHAR'}})
        """)
    finally:
        os.unlink(_txmap_tsv)
    gname = {i: g for g, i in gidx.items()}

    count_cols = [c[0] for c in con.sql(
        f"DESCRIBE SELECT * FROM read_parquet('{gene_df_file}')").fetchall()
        if c[0].startswith(("Uncon_", "Depth_"))]
    depth_cols = [c for c in count_cols if c.startswith("Depth_")]

    all_chroms = [r[0] for r in con.sql(
        f"SELECT DISTINCT Chrom FROM read_parquet('{genome_df_file}')").fetchall()]
    main = sorted(c for c in all_chroms if c and re.match(r"^(chr)?([0-9]+|[XYM]|MT)$", c))
    other = [c for c in all_chroms if c not in main]
    batches = [[c] for c in main] + ([other] if other else [])
    logging.info("Partitions: %d", len(batches))

    Path0 = os.path.dirname(os.path.abspath(output_file))
    os.makedirs(Path0, exist_ok=True)
    cnt_keep = ", ".join(f'"{c}"' for c in count_cols)
    cnt_cast = ", ".join(f'CAST("{c}" AS BIGINT) AS "{c}"' for c in count_cols)
    cnt_sum = ", ".join(f'SUM("{c}") AS "{c}"' for c in count_cols)
    dep_sum = " + ".join(f'SUM("{c}")' for c in depth_cols)
    header = ["Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif"] + count_cols

    if output_file.endswith(".gz"):
        # Parallel multi-member gzip at level 6: ~9x faster than the
        # single-threaded gzip module at the same level, and the same size
        # (level 9, Python's default, is slower AND larger on this data).
        fh = ParallelGzipWriter(output_file, level=6)
    else:
        fh = open(output_file, "w", newline="")
    w = csv.writer(fh, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_NONE)
    w.writerow(header)

    for batch in batches:
        inl = ",".join(q(c) for c in batch)
        rel_genes = [r[0] for r in con.sql(
            f"SELECT DISTINCT GeneName FROM txmap WHERE Chrom IN ({inl})").fetchall()]
        gin = ",".join(q(g) for g in rel_genes) or "''"

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE mapped_sites AS
            SELECT COALESCE(t.Chrom, gp.chrom) AS Chrom,
                   CAST(CASE WHEN t.Strand = '+' THEN
                        t.g_start + (gp."Pos" - 1 - t.tx_start) + 1
                        ELSE t.g_end - (gp."Pos" - 1 - t.tx_start) END AS BIGINT) AS Pos,
                   COALESCE(t.Strand, '.') AS "Strand",
                   gp.chrom AS GeneName,
                   CAST(gp."Pos" AS BIGINT) AS GenePos,
                   gp."Motif" AS Motif,
                   {cnt_cast}
            FROM read_parquet('{gene_df_file}') AS gp
            JOIN txmap t ON t.GeneName = gp.chrom
            WHERE gp.chrom IN ({gin})
              AND (gp."Pos" - 1 >= t.tx_start AND gp."Pos" - 1 < t.tx_end)
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE raw_sites AS
            SELECT Chrom, CAST(Pos AS BIGINT) AS Pos, "Strand",
                   CAST(NULL AS VARCHAR) AS GeneName,
                   CAST(NULL AS BIGINT) AS GenePos,
                   "Motif", {cnt_cast}
            FROM read_parquet('{genome_df_file}') AS rp
            WHERE Chrom IN ({inl})
        """)
        hav = f"(({dep_sum}) >= {int(min_depth)})" if depth_cols else "TRUE"
        rows = con.sql(f"""
            WITH u AS (SELECT * FROM mapped_sites UNION ALL BY NAME SELECT * FROM raw_sites),
            grp AS (
                SELECT Chrom, Pos, "Strand",
                       list(GeneName) FILTER (WHERE GeneName IS NOT NULL) AS gnames,
                       list(GenePos) FILTER (WHERE GenePos IS NOT NULL)  AS gposs,
                       MIN(Motif) AS Motif,
                       {cnt_sum}
                FROM u
                GROUP BY Chrom, Pos, "Strand"
                HAVING {hav}
            )
            SELECT Chrom, Pos, "Strand",
                   gnames, gposs, Motif,
                   {", ".join(f'"{c}"' for c in count_cols)}
            FROM grp
            ORDER BY Chrom, Pos, "Strand"
        """).fetchall()

        # Fast path: the csv.writer below runs with QUOTE_NONE, which RAISES if
        # any field contains the delimiter/quotechar - so every value it would
        # accept is delimiter-free and a plain "\t".join is byte-identical to
        # writerow() while roughly 5x faster (the per-row Python call was the
        # dominant cost of the whole remap; see the PHASES profiling).
        _s = str
        _n = ""          # csv renders None as ""
        buf = []
        add = buf.append
        for r in rows:
            chrom, pos, strand, gnames, gposs, motif, *_cnt = r
            if gnames:
                pairs = sorted(zip(map(gidx.__getitem__, gnames), gposs))
                gene_name = ";".join(map(gname.__getitem__, (p[0] for p in pairs)))
                gene_pos = ";".join(map(_s, (p[1] for p in pairs)))
            else:
                gene_name = gene_pos = ""
            add("\t".join((
                _n if chrom is None else _s(chrom),
                _n if pos is None else _s(pos),
                _n if strand is None else _s(strand),
                gene_name, gene_pos,
                _n if motif is None else _s(motif),
                *(_n if c is None else _s(c) for c in _cnt),
            )))
            if len(buf) >= 50000:
                fh.write("\n".join(buf) + "\n")
                buf.clear()
        if buf:
            fh.write("\n".join(buf) + "\n")

        con.execute("DROP TABLE IF EXISTS mapped_sites")
        con.execute("DROP TABLE IF EXISTS raw_sites")

    fh.close()
    logging.info("Remapping complete -> %s", output_file)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--transcript-file", required=True)
    ap.add_argument("-a", "--gene-file", required=True)
    ap.add_argument("-b", "--genome-file", required=True)
    ap.add_argument("-o", "--output-file", required=True)
    ap.add_argument("--min-depth", type=int, default=1)
    a = ap.parse_args()
    remap_and_join_files_parquet(a.gene_file, a.genome_file, a.transcript_file,
                                 a.output_file, a.min_depth)
