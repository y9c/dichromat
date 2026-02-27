#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2024 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Created: 2024-01-18 15:55


import gc
import logging
import os

import polars as pl

pl.enable_string_cache()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def read_file_by_polar(f: str, n: str) -> pl.DataFrame:
    logging.info(f"Reading {f}")
    
    # Peek at the header to decide format
    try:
        # Polars can read the first line of a gzipped file natively
        line = pl.read_csv(f, n_rows=1, has_header=False).item(0, 0)
        is_countmut = line.startswith("chrom")
    except Exception:
        is_countmut = False

    if is_countmut:
        # countmut format: chrom, pos, strand, motif, u0, u1, u2, m0, m1, m2
        df = (
            pl.scan_csv(f, separator="\t", has_header=True, schema_overrides={"chrom": pl.Utf8})
            .select([
                pl.col("chrom").alias("Chrom").cast(pl.Categorical),
                pl.col("pos").alias("Pos").cast(pl.UInt32),
                pl.col("strand").alias("Strand").cast(pl.Categorical),
                pl.col("motif").alias("Motif"),
                (pl.col("u1") + pl.col("u2")).alias(f"Uncon_{n}").cast(pl.UInt32),
                (pl.col("u1") + pl.col("m1") + pl.col("u2") + pl.col("m2")).alias(f"Depth_{n}").cast(pl.UInt32),
            ])
            .filter(pl.col(f"Depth_{n}") > 0)
        )
    else:
        # Legacy format (10 columns, no header)
        coltypes = {
            "Chrom": pl.Utf8,
            "Pos": pl.UInt32,
            "Strand": pl.Categorical,
            "Motif": pl.Utf8,
            "U0": pl.UInt32,
            "D0": pl.UInt32,
            "U1": pl.UInt32,
            "D1": pl.UInt32,
            "U2": pl.UInt32,
            "D2": pl.UInt32,
        }
        df = (
            pl.scan_csv(
                f,
                has_header=False,
                new_columns=list(coltypes.keys()),
                schema_overrides=coltypes,
                separator="\t",
            )
            .filter(pl.col("D1") > 0)
            .rename({"U1": f"Uncon_{n}", "D1": f"Depth_{n}"})
            .select([pl.col("Chrom").cast(pl.Categorical), "Pos", "Strand", "Motif", f"Uncon_{n}", f"Depth_{n}"])
        )

    try:
        df = df.sort(["Chrom", "Pos", "Strand"]).collect()
    except pl.exceptions.NoDataError:
        df = pl.DataFrame(
            None,
            schema={
                "Chrom": pl.Categorical,
                "Pos": pl.UInt32,
                "Strand": pl.Categorical,
                "Motif": pl.Utf8,
                f"Uncon_{n}": pl.UInt32,
                f"Depth_{n}": pl.UInt32,
            },
        )

    logging.info(f"Finished {f} ({n})")
    return df


def join_files_by_polar(
    files: list[str], names: list[str], requires: list[int], min_depth: int = 3
) -> pl.DataFrame:
    # Sort and unzip in one step
    files_ordered, names_ordered, requires_ordered = zip(
        *sorted(zip(files, names, requires), key=lambda x: -x[-1])
    )

    files_ordered = list(files_ordered)
    names_ordered = list(names_ordered)
    requires_ordered = list(requires_ordered)
    shinked = False
    df_joined = read_file_by_polar(files_ordered[0], names_ordered[0])
    for f, n, r in zip(files_ordered[1:], names_ordered[1:], requires_ordered[1:]):
        logging.info(f"Start to process: {f}, {n}, {r}")
        if not shinked and not r:
            df_joined = df_joined.fill_null(0).filter(
                pl.max_horizontal(pl.col("^Depth_.*$")) >= min_depth
            )
            shinked = True
        df = read_file_by_polar(f, n)

        df_joined = (
            df_joined.join(
                df,
                on=["Chrom", "Pos", "Strand"],
                how="full" if r else "left",
                coalesce=True,
                suffix="_right",
            )
            .with_columns(
                pl.when(pl.col("Motif").is_null())
                .then(pl.col("Motif_right"))
                .otherwise(pl.col("Motif"))
                .alias("Motif")
            )
            .drop("Motif_right")
        )

        del df
        gc.collect()

    return df_joined.fill_null(0).sort(["Chrom", "Pos", "Strand"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="*", type=str)
    parser.add_argument("--names", nargs="*", type=str)
    parser.add_argument("--requires", nargs="*", type=int)
    parser.add_argument("--min_depth", type=int, default=3)
    parser.add_argument("--output", type=str)
    args = parser.parse_args()

    df = join_files_by_polar(args.files, args.names, args.requires, args.min_depth)
    logging.info(f"Writing {args.output}")
    # Polars 1.3.0+ supports native gzip compression
    df.write_csv(args.output, separator="\t", compression="gzip" if args.output.endswith(".gz") else None)

