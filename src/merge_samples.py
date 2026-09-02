#!/usr/bin/env python3
"""duckdb (relational API) implementation of dichromat's sample-merging step.

Polars-free port of merge_samples.py, using duckdb's fluent API so the logic
reads like polars/SQL-lite instead of big raw-SQL strings.  Results are
byte-identical to the polars implementation (regression-checked).

  * each sample table (countmut native 8-column TSV / merged parquet) becomes
    a relation with Chrom, Pos, Strand, Motif, Uncon, Depth
  * required samples  -> FULL OUTER JOIN; optional -> LEFT JOIN  (USING keys)
  * Motif = first non-null across samples; counts zero-filled; min_depth filter
  * rows sorted by (Chrom, Pos, Strand); written as parquet

Usage (same interface as the polars script):
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


def file_relation(con, path, n):
    """One sample -> relation with Chrom, Pos, Strand and per-sample counts."""
    p = str(Path(path).resolve())
    m, u, d = f'"{n}__M"', f'"{n}__U"', f'"{n}__D"'

    if p.endswith(".parquet"):
        rel = con.from_parquet(p).project(
            "Chrom, Pos, Strand, Motif AS " + m
            + f", CAST(Uncon AS BIGINT) AS {u}"
            + f", CAST(Depth AS BIGINT) AS {d}")
        return rel.filter(f"{d} > 0")

    with open(p, errors="replace") as fh:
        first = fh.readline()
    if not first.lstrip().startswith("chrom"):
        raise ValueError(
            f"merge_samples: unrecognized pileup header in {p!r} "
            "(expected the countmut >= 0.2.2 native 8-column TSV: "
            "chrom pos strand motif u0 u1 m0 m1)")

    # countmut >= 0.2.2 native 8-column layout
    # (chrom pos strand motif u0 u1 m0 m1): group 1 (u1/m1) is the legacy
    # high-conversion gated set, so Uncon/Depth are computed from group 1
    # only -- site detection keeps the legacy definition.
    rel = con.from_csv_auto(
        p, delimiter="\t", header=True,
        columns={"chrom": "VARCHAR", "pos": "BIGINT", "strand": "VARCHAR",
                 "motif": "VARCHAR", "u0": "BIGINT", "u1": "BIGINT",
                 "m0": "BIGINT", "m1": "BIGINT"},
    ).project(
        "chrom AS Chrom, CAST(pos AS INTEGER) AS Pos,"
        " strand AS Strand, motif AS " + m
        + f", CAST(u1 AS BIGINT) AS {u}"
        + f", (CAST(u1 AS BIGINT) + CAST(m1 AS BIGINT)) AS {d}")
    return rel.filter(f"{d} > 0")


def merge_to_parquet(files, names, requires, output_file, min_depth=3):
    con = duckdb.connect()
    # required samples first (matches the polars join order)
    ordered = sorted(zip(files, names, requires), key=lambda x: -x[2])

    rels = []
    for (f, n, _r) in ordered:
        rels.append(file_relation(con, f, n).set_alias(f"t{n}"))

    base = rels[0]
    filtered_optional = False
    for i in range(1, len(rels)):
        how = "outer" if ordered[i][2] == 1 else "left"
        # USING-style join -> (Chrom, Pos, Strand) are coalesced across sides
        base = base.join(rels[i], ["Chrom", "Pos", "Strand"], how=how)
        # polars applies min_depth right after the FIRST optional sample is
        # joined (before later optional samples); replicate that exact point.
        if ordered[i][2] == 0 and not filtered_optional:
            dlist = ", ".join(f'"{n}__D"' for _f, n, _r in ordered[: i + 1])
            base = base.filter(f"GREATEST({dlist}) >= {int(min_depth)}")
            filtered_optional = True

    exprs = [
        "Chrom", "Pos", "Strand",
        "COALESCE(" + ", ".join(f'"{n}__M"' for _f, n, _r in ordered) + ") AS Motif",
    ]
    for _f, n, _r in ordered:
        exprs.append(f'COALESCE("{n}__U", 0) AS "Uncon_{n}"')
        exprs.append(f'COALESCE("{n}__D", 0) AS "Depth_{n}"')

    final = base.project(", ".join(exprs)).order('Chrom, Pos, Strand')

    out = str(Path(output_file).resolve())
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    final.write_parquet(out)
    logging.info("Merged %d samples -> %s", len(ordered), output_file)


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
