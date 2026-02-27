#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2024 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Created: 2024-06-01 15:32


import logging

import polars as pl
from scipy.stats import binom

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def parse_df(input_file, names):
    # read input file
    df_input = (
        pl.scan_csv(
            input_file,
            separator="\t",
            schema_overrides={"Chrom": pl.Utf8, "Pos": pl.UInt32},
        )
        .with_columns(
            Uncon=pl.sum_horizontal([pl.col(f"Uncon_{n}") for n in names]),
            Depth=pl.sum_horizontal([pl.col(f"Depth_{n}") for n in names]),
        )
        .select(
            ["Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif", "Depth", "Uncon"]
        )
    )
    logging.info(f"Finish reading input for {names}")

    min_depth = 20

    df = (
        df_input.with_columns(
            M3=pl.col("Motif")
            .str.slice(14, 3)
            .str.to_uppercase()
            .str.replace_all("T", "U"),
        )
        .with_columns(
            p=pl.struct(["Uncon", "Depth"]).map_batches(
                lambda s: pl.Series(binom.sf(s.struct.field("Uncon"), s.struct.field("Depth"), 0.01, loc=1)),
                return_dtype=pl.Float64
            ),
        )
        .with_columns(
            pl.when((pl.col("Uncon") <= 0) | (pl.col("Depth") <= 0))
            .then(1.0)
            .otherwise(pl.col("p"))
            .alias("p")
        )
        .select(
            [
                "Chrom",
                "Pos",
                "Strand",
                "GeneName",
                "GenePos",
                "Motif",
                "M3",
                "Depth",
                "Uncon",
                "p",
            ]
        )
        .filter((pl.col("Depth") >= min_depth) & (pl.col("Strand") != "."))
    ).collect()
    return df


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", type=str, help="Input file")
    ap.add_argument("-n", "--names", nargs="+", help="Names of samples")
    ap.add_argument("-o", "--output", type=str, help="Output file 1 (filter by depth)")
    args = ap.parse_args()

    # input_file = "merged.tsv.gz"
    # output_f1 = "d20.parquet"
    # output_f2 = "d20_p30.parquet"

    df = parse_df(args.input, args.names)
    df.write_parquet(args.output)
    # df.filter(pl.col("p") < 0.001).write_parquet(output_f2)
