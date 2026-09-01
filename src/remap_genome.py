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

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def parse_transcript(tx_file):
    order, seen, exons = [], set(), []
    raw = open(tx_file).read().splitlines()
    if not raw:
        return [], [], {}
    for line in raw[1:]:
        gid = line.split("\t")[0]
        if gid not in seen:
            seen.add(gid)
            order.append(gid)
    for line in raw[1:]:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 4:
            continue
        gid, chrom, strand, spans = parts[0], parts[1], parts[2], parts[3]
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


def remap_and_join_files_parquet(gene_df_file, genome_df_file, transcript_file,
                                 output_file, min_depth=1):
    gene_order, exons, gidx = parse_transcript(transcript_file)
    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE txmap(GeneName VARCHAR, Chrom VARCHAR,"
                " Strand VARCHAR, g_start BIGINT, g_end BIGINT,"
                " tx_start BIGINT, tx_end BIGINT)")
    con.executemany("INSERT INTO txmap VALUES (?,?,?,?,?,?,?)", exons)
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
        fh = gzip.open(output_file, "wt", newline="")
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

        for r in rows:
            chrom, pos, strand, gnames, gposs, motif, *_cnt = r
            if gnames:
                pairs = sorted((gidx[gn], gposs[i]) for i, gn in enumerate(gnames))
                gene_name = ";".join(gname[gi] for gi, _ in pairs)
                gene_pos = ";".join(str(p) for _, p in pairs)
            else:
                gene_name, gene_pos = "", ""
            w.writerow([chrom, pos, strand, gene_name, gene_pos, motif] + list(_cnt))

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
