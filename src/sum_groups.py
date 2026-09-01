#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""duckdb version of sum_groups.py (polars-free), results identical.

Reads the merged per-sample depth table (TSV), sums Uncon_*/Depth_* across
samples, builds the trinucleotide M3, computes the binomial p-value (keeps
scipy.stats.binom - scipy remains in the image) and writes parquet.

Replace the polars implementation in the pipeline: same columns, order,
filtering (Depth>=20, Strand != '.') and p semantics.
"""

import logging

import duckdb
import numpy as np
from scipy.stats import binom

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

MIN_DEPTH = 20
P_TEMPLATE = 0.01


def parse_df(input_file, names):
    con = duckdb.connect()

    def _sum(col):
        return "(" + " + ".join(f"COALESCE(\"{col}_{n}\", 0)" for n in names) + ")"

    q = f"""
        SELECT "Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif",
               {_sum('Depth')} AS Depth,
               {_sum('Uncon')} AS Uncon
        FROM read_csv_auto('{input_file.replace("'", "''")}',
                           delim='\\t', header=true)
        WHERE {_sum('Depth')} >= {MIN_DEPTH} AND "Strand" <> '.'
        """
    rows = con.execute(q).fetchall()  # file order preserved (single scan)
    cols = ["Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif",
            "M3", "Depth", "Uncon", "p"]
    if rows:
        uncon = np.array([r[7] for r in rows], dtype=np.int64)   # Depth=6, Uncon=7
        depth = np.array([r[6] for r in rows], dtype=np.int64)
        motif = [str(r[5]) for r in rows]
        m3 = [m[14:17].upper().replace("T", "U") for m in motif]
        p = binom.sf(uncon, depth, P_TEMPLATE, loc=1).astype(float)
        p = np.where((uncon <= 0) | (depth <= 0), 1.0, p)

        def _lit(x):
            if x is None:
                return "NULL"
            if isinstance(x, bool):
                return "true" if x else "false"
            if isinstance(x, (int, np.integer)):
                return str(int(x))
            if isinstance(x, float):
                return repr(float(x))
            return "'" + str(x).replace("'", "''") + "'"

        vals = []
        for i, r in enumerate(rows):
            vals.append("(" + ", ".join(
                _lit(v) for v in [r[0], r[1], r[2], r[3], r[4], r[5],
                                  m3[i], int(depth[i]), int(uncon[i]),
                                  float(p[i])]) + ")")
        rel = con.sql(
            "SELECT * FROM (VALUES\n" + ",\n".join(vals) + ") t("
            + ", ".join(f'"{c}"' for c in cols) + ")")
    else:
        rel = con.sql(
            "SELECT NULL::VARCHAR AS \"Chrom\", NULL::BIGINT AS \"Pos\", "
            "NULL::VARCHAR AS \"Strand\", NULL::VARCHAR AS \"GeneName\", "
            "NULL::BIGINT AS \"GenePos\", NULL::VARCHAR AS \"Motif\", "
            "NULL::VARCHAR AS \"M3\", NULL::BIGINT AS \"Depth\", "
            "NULL::BIGINT AS \"Uncon\", NULL::DOUBLE AS \"p\" WHERE false")
    return rel


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", type=str, help="Input file")
    ap.add_argument("-n", "--names", nargs="+", help="Names of samples")
    ap.add_argument("-o", "--output", type=str, help="Output parquet")
    args = ap.parse_args()

    rel = parse_df(args.input, args.names)
    rel.write_parquet(args.output)
    logging.info("Wrote %s", args.output)
