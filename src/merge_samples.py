#!/usr/bin/env python
import logging
import os
import polars as pl
import re
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def get_file_format(f):
    """Peek at the header to decide format."""
    try:
        line = pl.read_csv(f, n_rows=1, has_header=False).item(0, 0)
        return line.startswith("chrom")
    except:
        return False

def get_lazy_plan(f, n, is_countmut):
    """Build a lazy plan for a single sample file."""
    if is_countmut:
        return (
            pl.scan_csv(f, separator="\t", has_header=True, schema_overrides={"chrom": pl.Utf8})
            .select([
                pl.col("chrom").alias("Chrom"),
                pl.col("pos").alias("Pos").cast(pl.UInt32),
                pl.col("strand").alias("Strand"),
                pl.col("motif").alias("Motif"),
                (pl.col("u1") + pl.col("u2")).alias(f"Uncon_{n}").cast(pl.UInt32),
                (pl.col("u1") + pl.col("m1") + pl.col("u2") + pl.col("m2")).alias(f"Depth_{n}").cast(pl.UInt32),
            ])
            .filter(pl.col(f"Depth_{n}") > 0)
        )
    else:
        coltypes = {"Chrom": pl.Utf8, "Pos": pl.UInt32, "Strand": pl.Utf8, "Motif": pl.Utf8, "U0": pl.UInt32, "D0": pl.UInt32, "U1": pl.UInt32, "D1": pl.UInt32, "U2": pl.UInt32, "D2": pl.UInt32}
        return (
            pl.scan_csv(f, has_header=False, new_columns=list(coltypes.keys()), schema_overrides=coltypes, separator="\t")
            .filter(pl.col("D1") > 0)
            .rename({"U1": f"Uncon_{n}", "D1": f"Depth_{n}"})
            .select(["Chrom", "Pos", "Strand", "Motif", f"Uncon_{n}", f"Depth_{n}"])
        )

def merge_samples_batched(files, names, requires, output_file, min_depth=3):
    pl.enable_string_cache()
    
    # 1. Identify all unique chromosomes across all files
    logging.info("Identifying unique chromosomes across all samples...")
    all_chroms_set = set()
    file_formats = {}
    for f, n in zip(files, names):
        fmt = get_file_format(f)
        file_formats[n] = fmt
        chroms = pl.scan_csv(f, separator="\t", has_header=fmt, infer_schema_length=0).select(pl.col(pl.first().alias("Chrom"))).unique().collect().get_column("Chrom").to_list()
        all_chroms_set.update(chroms)
    
    all_chroms = sorted(list(all_chroms_set))
    main_chroms = sorted([c for c in all_chroms if re.match(r'^(chr)?([0-9]+|[XYM]|MT)$', c)])
    other_contigs = [c for c in all_chroms if c not in main_chroms]
    batches = [[c] for c in main_chroms]
    if other_contigs: batches.append(other_contigs)
    
    logging.info(f"Processing {len(all_chroms)} chroms in {len(batches)} batches")

    first_batch = True
    temp_output = output_file.replace(".gz", "") if output_file.endswith(".gz") else output_file + ".tmp"
    
    # Order files by 'requires' to handle join logic (Full Join for required, Left Join for optional)
    ordered_info = sorted(zip(files, names, requires), key=lambda x: -x[2])
    
    for chrom_batch in batches:
        logging.info(f"Processing batch: {chrom_batch[0]}...")
        
        df_batch = None
        shinked = False
        
        for f, n, r in ordered_info:
            is_countmut = file_formats[n]
            # Scan only this batch's chroms
            lf = get_lazy_plan(f, n, is_countmut).filter(pl.col("Chrom").is_in(chrom_batch))
            
            # Perform early filtering for optional samples to keep memory low
            if not r and not shinked and df_batch is not None:
                df_batch = df_batch.filter(pl.max_horizontal(pl.col("^Depth_.*$")) >= min_depth)
                shinked = True
            
            df_sample = lf.collect()
            if df_sample.is_empty(): continue
            
            if df_batch is None:
                df_batch = df_sample
            else:
                df_batch = df_batch.join(
                    df_sample, 
                    on=["Chrom", "Pos", "Strand"], 
                    how="full" if r else "left", 
                    coalesce=True, 
                    suffix="_right"
                ).with_columns(
                    pl.when(pl.col("Motif").is_null()).then(pl.col("Motif_right")).otherwise(pl.col("Motif")).alias("Motif")
                ).drop("Motif_right")
            
            import gc; gc.collect()

        if df_batch is not None and not df_batch.is_empty():
            df_batch = df_batch.fill_null(0).sort(["Chrom", "Pos", "Strand"])
            # Append to file
            with open(temp_output, "ab") as f_out:
                df_batch.write_csv(f_out, separator="\t", include_header=first_batch)
            first_batch = False

    if not first_batch and output_file.endswith(".gz"):
        logging.info(f"Compressing to {output_file}")
        os.system(f"gzip -f {temp_output}")
        if temp_output != output_file: os.rename(temp_output + ".gz", output_file)
    elif not first_batch:
        os.rename(temp_output, output_file)
        
    logging.info("Merge complete.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="*", type=str)
    parser.add_argument("--names", nargs="*", type=str)
    parser.add_argument("--requires", nargs="*", type=int)
    parser.add_argument("--min_depth", type=int, default=3)
    parser.add_argument("--output", type=str)
    args = parser.parse_args()
    merge_samples_batched(args.files, args.names, args.requires, args.output, args.min_depth)
