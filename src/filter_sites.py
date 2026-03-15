#!/usr/bin/env python
import logging
import os
import polars as pl
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def scan_input(f):
    if f.endswith(".parquet"):
        return pl.scan_parquet(f)
    else:
        return pl.scan_csv(f, separator="\t", infer_schema_length=None)

def filter_sites(input_file, output_file, min_depth=5):
    pl.enable_string_cache()
    logging.info(f"Filtering sites from {input_file} -> {output_file}")
    
    lf = scan_input(input_file)
    
    # Simple filtering based on depth across all samples
    lf_filtered = lf.filter(pl.sum_horizontal(pl.col("^Depth_.*$")) >= min_depth).sort(["Chrom", "Pos", "Strand"])
    
    if output_file.endswith(".parquet"):
        lf_filtered.sink_parquet(output_file, compression="zstd")
    elif output_file.endswith(".gz"):
        lf_filtered.collect().write_csv(output_file, separator="\t", quote_style="never", compression="gzip")
    else:
        lf_filtered.collect().write_csv(output_file, separator="\t", quote_style="never")
        
    logging.info("Filtering complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("--min-depth", type=int, default=5)
    args = parser.parse_args()
    filter_sites(args.input, args.output, args.min_depth)
