#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2024 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Created: 2024-05-05 15:00
import gc
import logging
import os

os.environ["POLARS_MAX_THREADS"] = "24"

import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

pl.enable_string_cache()


def parse_tx_file_to_df(tx_file):
    # Parse transcript.tsv into a DataFrame of individual exons
    records = []
    with open(tx_file, "r") as f:
        header = f.readline().strip().split("\t")
        col2index = {col: idx for idx, col in enumerate(header)}
        selected_cols = [
            col2index[col] for col in ["gene_id", "chrom", "strand", "spans"]
        ]
        for line in f:
            line = line.strip()
            parts = line.split("\t")
            gene_id, chrom, strand, spans = [parts[idx] for idx in selected_cols]
            
            exon_len = 0
            for span in spans.split(","):
                start_str, end_str = span.split("-", maxsplit=1)
                # 0-based genomic coordinates
                g_start, g_end = int(start_str) - 1, int(end_str)
                length = g_end - g_start
                # Transcript coordinates relative to the exon
                tx_start = exon_len
                tx_end = exon_len + length
                exon_len += length
                
                records.append({
                    "GeneName": gene_id,
                    "Chrom": chrom,
                    "Strand": strand,
                    "g_start": g_start,
                    "g_end": g_end,
                    "tx_start": tx_start,
                    "tx_end": tx_end,
                })
                
    return pl.DataFrame(records, schema={
        "GeneName": pl.String,
        "Chrom": pl.String,
        "Strand": pl.String,
        "g_start": pl.Int64,
        "g_end": pl.Int64,
        "tx_start": pl.Int64,
        "tx_end": pl.Int64,
    })


def remap_and_join_files(
    gene_df_file: str,
    genome_df_file: str,
    transcript_file: str,
    output_file: str,
    min_depth: int = 5,
):
    logging.info("Parsing transcript mapping file into Polars DataFrame")
    exons_df = parse_tx_file_to_df(transcript_file)
    
    # Read gene df
    logging.info("Reading transcript pileup")
    df1 = (
        pl.scan_csv(gene_df_file, separator="\t", has_header=True, infer_schema_length=None)
        .with_columns([
            pl.col("Chrom").cast(pl.Utf8),
            pl.col("Pos").cast(pl.UInt32),
        ])
        .rename({"Chrom": "GeneName", "Pos": "GenePos_1based"})
        .drop(["Strand"])
        .with_columns(
            (pl.col("GenePos_1based") - 1).alias("GenePos_0based")
        )
    ).collect()
    
    # Vectorized mapping using a join
    logging.info("Vectorized liftover to genomic coordinates")
    
    df1 = (
        df1.join(exons_df, on="GeneName", how="left")
        .filter(
            pl.col("Chrom").is_null() |  # Keep unmapped ones to fall back
            ((pl.col("GenePos_0based") >= pl.col("tx_start")) & 
             (pl.col("GenePos_0based") < pl.col("tx_end")))
        )
        .with_columns(
            offset=(pl.col("GenePos_0based") - pl.col("tx_start")),
        )
        .with_columns(
            MappedPos=pl.when(pl.col("Strand") == "+")
            .then(pl.col("g_start") + pl.col("offset"))
            .when(pl.col("Strand") == "-")
            .then(pl.col("g_end") - pl.col("offset") - 1)
            .otherwise(pl.col("GenePos_0based"))  # Fallback for unmapped
        )
        .with_columns(
            Chrom=pl.col("Chrom").fill_null(pl.col("GeneName")),
            Pos=pl.col("MappedPos") + 1,
            Strand=pl.col("Strand").fill_null("."),
            GenePos=pl.col("GenePos_1based")
        )
        .select(
            ["Chrom", "Pos", "Strand", "GeneName", "GenePos"]
            + [c for c in df1.columns if c not in ["GeneName", "GenePos_1based", "GenePos_0based", "Chrom", "Pos", "Strand"]]
        )
        .with_columns(
            Pos=pl.col("Pos").cast(pl.UInt32),
            Strand=pl.col("Strand").cast(pl.Categorical),
            GeneName=pl.col("GeneName").cast(pl.String),
            GenePos=pl.col("GenePos").cast(pl.Int32),
            Motif=pl.col("Motif").cast(pl.Utf8),
        )
    )
    
    logging.info(f"Loaded and mapped {len(df1)} transcript positions.")

    # Read genome df
    df2 = pl.scan_csv(
        genome_df_file,
        separator="\t",
        has_header=True,
        infer_schema_length=None,
        schema_overrides={
            "Chrom": pl.String,
            "Pos": pl.UInt32,
            "Strand": pl.Categorical,
            "Motif": pl.Utf8,
        },
    ).collect()
    # insert columns GeneName and GenePos with None values
    df2 = df2.insert_column(
        3, pl.lit(None).cast(pl.String).alias("GeneName")
    ).insert_column(4, pl.lit(None).cast(pl.Int32).alias("GenePos"))
    
    logging.info(f"Loaded {len(df2)} genome positions.")

    df = df1.vstack(df2)
    del df1, df2, exons_df
    gc.collect()
    logging.info(f"Joined {len(df)} total positions.")

    # Perform final group_by and filter
    logging.info("Grouping and filtering joined data")
    df = (
        df.group_by(["Chrom", "Pos", "Strand"])
        .agg(
            pl.col("GeneName")
            .filter(pl.col("GeneName").is_first_distinct())
            .drop_nulls()
            .str.join(";"),
            pl.col("GenePos")
            .filter(pl.col("GeneName").is_first_distinct())
            .drop_nulls()
            .cast(pl.String)
            .str.join(";"),
            pl.col("Motif").first(),
            pl.exclude(
                ["Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif"]
            ).sum(),
        )
        .filter(pl.sum_horizontal(pl.col("^Depth_.*$")) >= min_depth)
        .sort(["Chrom", "Pos", "Strand"])
    )

    logging.info(f"Writing final merged results to {output_file}")
    df.write_csv(output_file, separator="\t", compression="gzip" if output_file.endswith(".gz") else None)

    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--transcript-file", type=str, required=True)
    parser.add_argument("-a", "--gene-file", type=str, required=True)
    parser.add_argument("-b", "--genome-file", type=str, required=True)
    parser.add_argument("-o", "--output-file", type=str, required=True)
    parser.add_argument("--min-depth", type=int, default=5, 
                        help="Minimum total depth across all libraries (default: 5)")
    args = parser.parse_args()

    df = remap_and_join_files(
        args.gene_file, args.genome_file, args.transcript_file, args.output_file,
        args.min_depth
    )
