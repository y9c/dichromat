#!/usr/bin/env python
import logging
import os
import gzip
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def parse_tx_file_to_df(tx_file):
    records = []
    with open(tx_file, "r") as f:
        header = f.readline().strip().split("\t")
        col2index = {col: idx for idx, col in enumerate(header)}
        selected_cols = [col2index[col] for col in ["gene_id", "chrom", "strand", "spans"]]
        for line in f:
            parts = line.strip().split("\t")
            gene_id, chrom, strand, spans = [parts[idx] for idx in selected_cols]
            exon_len = 0
            for span in spans.split(","):
                start_str, end_str = span.split("-", maxsplit=1)
                g_start, g_end = int(start_str) - 1, int(end_str)
                length = g_end - g_start
                tx_start, tx_end = exon_len, exon_len + length
                exon_len += length
                records.append({
                    "GeneName": gene_id, "Chrom": chrom, "Strand": strand,
                    "g_start": g_start, "g_end": g_end, "tx_start": tx_start, "tx_end": tx_end,
                })
    return pl.DataFrame(records, schema={
        "GeneName": pl.String, "Chrom": pl.String, "Strand": pl.String,
        "g_start": pl.Int64, "g_end": pl.Int64, "tx_start": pl.Int64, "tx_end": pl.Int64,
    })

def remap_and_join_files_final(gene_df_file, genome_df_file, transcript_file, output_file, min_depth=5):
    pl.enable_string_cache()
    
    logging.info("Step 1: Parsing transcript mapping file")
    exons_df = parse_tx_file_to_df(transcript_file)
    unique_genes = exons_df.get_column("GeneName").unique().sort()
    gene_map_df = pl.DataFrame({
        "GeneName": unique_genes,
        "GeneID": pl.Series(range(len(unique_genes)), dtype=pl.Int32)
    })
    exons_df = exons_df.join(gene_map_df, on="GeneName").drop("GeneName").lazy()
    gene_map_df = gene_map_df.lazy()
    
    logging.info("Step 2: Building Lazy Plans")
    df1_lazy = (
        pl.scan_csv(gene_df_file, separator="\t", infer_schema_length=None)
        .rename({"Chrom": "GeneName", "Pos": "GenePos_1based"})
        .drop(["Strand"])
        .with_columns((pl.col("GenePos_1based") - 1).alias("GenePos_0based"))
        .join(gene_map_df, on="GeneName", how="inner")
        .join(exons_df, on="GeneID", how="inner")
        .filter(
            ((pl.col("GenePos_1based") - 1 >= pl.col("tx_start")) & 
             (pl.col("GenePos_1based") - 1 < pl.col("tx_end")))
        )
        .with_columns(
            Pos=pl.when(pl.col("Strand") == "+")
                .then(pl.col("g_start") + (pl.col("GenePos_1based") - 1 - pl.col("tx_start")) + 1)
                .otherwise(pl.col("g_end") - (pl.col("GenePos_1based") - 1 - pl.col("tx_start"))),
            GenePos=pl.col("GenePos_1based").cast(pl.UInt32)
        )
        .with_columns([
            pl.col("Chrom").fill_null(pl.col("GeneName")),
            pl.col("Pos").cast(pl.UInt32), # EXPLICIT CAST TO MATCH DF2
            pl.col("Strand").fill_null(".")
        ])
    )
    
    df2_lazy = (
        pl.scan_csv(genome_df_file, separator="\t", infer_schema_length=None)
        .with_columns([
            pl.col("Pos").cast(pl.UInt32), # EXPLICIT CAST TO MATCH DF1
            pl.lit(None).cast(pl.Int32).alias("GeneID"),
            pl.lit(None).cast(pl.UInt32).alias("GenePos"),
        ])
    )
    
    schema = df1_lazy.collect_schema()
    count_cols = [c for c in schema.names() if c.startswith(("Uncon_", "Depth_"))]
    required_cols = ["Chrom", "Pos", "Strand", "GeneID", "GenePos", "Motif"] + count_cols
    
    df_combined = (
        pl.concat([
            df1_lazy.select(required_cols).with_columns([pl.col(c).cast(pl.Int64) for c in count_cols]),
            df2_lazy.select(required_cols).with_columns([pl.col(c).cast(pl.Int64) for c in count_cols])
        ], how="diagonal")
        .with_columns([
            pl.col("Chrom").cast(pl.Categorical),
            pl.col("Strand").cast(pl.Categorical),
            pl.col("Motif").cast(pl.Categorical),
        ])
        .group_by(["Chrom", "Pos", "Strand"])
        .agg([
            pl.col("GeneID").drop_nulls().unique().sort().alias("ids"),
            pl.col("GenePos").drop_nulls().unique().sort().alias("gpos"),
            pl.col("Motif").sort().first(),
            pl.exclude(["Chrom", "Pos", "Strand", "GeneID", "GenePos", "Motif", "ids", "gpos"]).sum()
        ])
        .filter(pl.sum_horizontal(pl.col("^Depth_.*$")) >= min_depth)
    )

    logging.info("Step 3: Executing with Streaming engine")
    result = df_combined.collect(engine="streaming")
    
    logging.info("Step 4: Vectorized Formatting")
    gene_names_list = unique_genes.to_list()
    def format_names(l):
        if l is None or len(l) == 0: return '""'
        return ";".join([gene_names_list[i] for i in l])
    def format_pos(l):
        if l is None or len(l) == 0: return '""'
        return ";".join([str(p) for p in l])

    result = (
        result.with_columns([
            pl.col("ids").map_elements(format_names, return_dtype=pl.String).alias("GeneName"),
            pl.col("gpos").map_elements(format_pos, return_dtype=pl.String).alias("GenePos")
        ])
        .drop(["ids", "gpos"])
        .select(["Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif"] + count_cols)
        .sort(["Chrom", "Pos", "Strand"])
    )
    
    logging.info(f"Step 5: Writing results to {output_file}")
    if output_file.endswith(".gz"):
        with gzip.open(output_file, "wb") as f:
            result.write_csv(f, separator="\t", quote_style="never")
    else:
        result.write_csv(output_file, separator="\t", quote_style="never")
    return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--transcript-file", required=True)
    parser.add_argument("-a", "--gene-file", required=True)
    parser.add_argument("-b", "--genome-file", required=True)
    parser.add_argument("-o", "--output-file", required=True)
    parser.add_argument("--min-depth", type=int, default=1)
    args = parser.parse_args()
    remap_and_join_files_final(args.gene_file, args.genome_file, args.transcript_file, args.output_file, args.min_depth)
