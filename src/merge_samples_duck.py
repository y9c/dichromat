#!/usr/bin/env python3
"""duckdb implementation of dichromat's sample-merging step.

Replaces the polars version of merge_samples.py so polars can be dropped from
the image.  Merge semantics are kept IDENTICAL to the polars implementation:

  * per-sample tables (countmut TSV / legacy TSV / already-merged parquet)
  * required samples FULL OUTER JOIN, optional samples LEFT JOIN on
    (Chrom, Pos, Strand), Motif coalesced to the first non-null sample
  * after the first optional sample: filter out sites where max(Depth_*) < min_depth
  * missing counts zero-filled; rows sorted by (Chrom, Pos, Strand)

Usage (same interface as the old script):
  merge_samples.py --files F1 [F2 ...] --names N1 [N2 ...]
                   --requires 0|1 ... --output OUT.parquet --min_depth 3
"""

import argparse
import logging
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

CHROM_POS_COLS = "Chrom", "Pos", "Strand", "Motif"


def _file_table(con, path, name):
    """Register a view `t_{name}` with columns Chrom,Pos,Strand,Motif,Uncon,Depth."""
    p = str(Path(path).resolve())
    if p.endswith(".parquet"):
        q = f"""
            SELECT Chrom, Pos, Strand, Motif,
                   CAST("Uncon" AS BIGINT) AS Uncon,
                   CAST("Depth" AS BIGINT) AS Depth
            FROM read_parquet('{p.replace("'", "''")}')
            """
    else:
        first = None
        with open(p, errors="replace") as fh:
            first = fh.readline().rstrip("\n")
        header = first.split("\t")
        if header and header[0].strip().startswith("chrom"):
            # countmut output: chrom pos strand motif u1 m1 u2 m2
            cols = ('"u1","m1","u2","m2", "chrom","pos","strand","motif"')
            q = f"""
                SELECT "chrom" AS Chrom, CAST("pos" AS INTEGER) AS Pos,
                       "strand" AS Strand, "motif" AS Motif,
                       CAST("u1" AS BIGINT) + CAST("u2" AS BIGINT) AS Uncon,
                       CAST("u1" AS BIGINT) + CAST("m1" AS BIGINT)
                       + CAST("u2" AS BIGINT) + CAST("m2" AS BIGINT) AS Depth
                FROM read_csv_auto('{p.replace("'", "''")}',
                                   delim='\\t', header=true,
                                   columns={{'chrom':'VARCHAR','pos':'BIGINT',
                                             'strand':'VARCHAR','motif':'VARCHAR',
                                             'u1':'BIGINT','m1':'BIGINT',
                                             'u2':'BIGINT','m2':'BIGINT'}})
                WHERE CAST("u1" AS BIGINT) + CAST("u2" AS BIGINT)
                      + CAST("m1" AS BIGINT) + CAST("m2" AS BIGINT) > 0
                """
        else:
            # legacy: Chrom Pos Strand Motif U0 D0 U1 D1 U2 D2  (U1/D1 = unconv/depth)
            q = f"""
                SELECT "col0" AS Chrom, CAST("col1" AS INTEGER) AS Pos,
                       "col2" AS Strand, "col3" AS Motif,
                       CAST("col6" AS BIGINT) AS Uncon,
                       CAST("col7" AS BIGINT) AS Depth
                FROM read_csv_auto('{p.replace("'", "''")}',
                                   delim='\\t', header=false,
                                   columns={{'col0':'VARCHAR','col1':'BIGINT',
                                             'col2':'VARCHAR','col3':'VARCHAR',
                                             'col4':'BIGINT','col5':'BIGINT',
                                             'col6':'BIGINT','col7':'BIGINT',
                                             'col8':'BIGINT','col9':'BIGINT'}})
                WHERE CAST("col7" AS BIGINT) > 0
                """
    con.execute(
        f'CREATE OR REPLACE VIEW {name} AS SELECT * FROM ({q}) WHERE Depth > 0')


def merge_to_parquet(files, names, requires, output_file, min_depth=3):
    con = duckdb.connect()
    ordered = sorted(zip(files, names, requires), key=lambda x: -x[2])
    alias = []
    for i, (_f, n, _r) in enumerate(ordered):
        a = f"t{i}_{n}"
        alias.append(a)
        _file_table(con, _f, a)

    # base CTE: Chrom/Pos/Strand + a per-sample Motif + count column each
    cte_cols = []
    for a, (_f, n, _r) in zip(alias, ordered):
        cte_cols.append(f'{a}.Motif AS "{n}__M"')
        cte_cols.append(f'{a}.Uncon AS "{n}__U"')
        cte_cols.append(f'{a}.Depth AS "{n}__D"')
    base_select = (f"SELECT Chrom, Pos, Strand,\n"
                   + ",\n".join(cte_cols) + f"\nFROM {alias[0]}")
    joins = ""
    for i in range(1, len(alias)):
        jt = "FULL OUTER JOIN" if ordered[i][2] == 1 else "LEFT JOIN"
        joins += f" {jt} {alias[i]} USING (Chrom, Pos, Strand)\n"
    cte = f"WITH base AS (\n{base_select}\n{joins})\n"

    # output columns in the same order/name as the polars implementation
    parts = ['base."Chrom" AS Chrom', 'base."Pos" AS Pos',
             'base."Strand" AS Strand',
             "COALESCE(" + ", ".join(f'base."{n}__M"' for _f, n, _r in ordered)
             + ") AS Motif"]
    for _f, n, _r in ordered:
        parts.append(f'COALESCE(base."{n}__U", 0) AS "Uncon_{n}"')
        parts.append(f'COALESCE(base."{n}__D", 0) AS "Depth_{n}"')
    sel = "SELECT\n" + ",\n".join(parts) + "\nFROM base\n"
    where = ""
    if any(r == 0 for _f, _n, r in ordered) and len(ordered) > 1:
        dargs = ", ".join(f'base."{n}__D"' for _f, n, _r in ordered)
        where = f"WHERE GREATEST({dargs}) >= {int(min_depth)}\n"
    order = 'ORDER BY base."Chrom", base."Pos", base."Strand"\n'
    query = cte + sel + where + order

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    logging.info("Merging %d samples -> %s", len(ordered), output_file)
    con.execute(
        f"COPY ({query}) TO '{Path(output_file).resolve()}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD)")
    rows = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    logging.info("Merge complete: %d rows", rows)
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="*", type=str)
    parser.add_argument("--names", nargs="*", type=str)
    parser.add_argument("--requires", nargs="*", type=int)
    parser.add_argument("--min_depth", type=int, default=3)
    parser.add_argument("--output", type=str)
    args = parser.parse_args()
    merge_to_parquet(args.files, args.names, args.requires, args.output,
                     args.min_depth)
