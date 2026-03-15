#!/usr/bin/env python
import logging
import os
import polars as pl
import re
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def get_file_format(f):
    try:
        line = pl.read_csv(f, n_rows=1, has_header=False).item(0, 0)
        return line.startswith("chrom")
    except:
        return False

def get_lazy_plan(f, n, is_countmut):
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
    
    # 1. Identify all unique chromosomes
    logging.info("Step 1: Identifying and batching chromosomes...")
    all_chroms_set = set()
    file_formats = {}
    for f, n in zip(files, names):
        fmt = get_file_format(f)
        file_formats[n] = fmt
        # Fast scan for unique chroms
        chroms = pl.scan_csv(f, separator="\t", has_header=fmt, infer_schema_length=0).select(pl.col(pl.first().alias("Chrom"))).unique().collect().get_column("Chrom").to_list()
        all_chroms_set.update(chroms)
    
    all_chroms = sorted(list(all_chroms_set))
    main_chroms = sorted([c for c in all_chroms if re.match(r'^(chr)?([0-9]+|[XYM]|MT)$', c)])
    other_contigs = [c for c in all_chroms if c not in main_chroms]
    
    # COARSE BATCHING: Group main chroms in batches of 5 to reduce file scans
    batches = []
    chunk_size = 5
    for i in range(0, len(main_chroms), chunk_size):
        batches.append(main_chroms[i:i+chunk_size])
    if other_contigs:
        batches.append(other_contigs)
    
    logging.info(f"Processing {len(all_chroms)} chroms in {len(batches)} coarse batches")

    first_batch = True
    temp_output = output_file.replace(".gz", "") if output_file.endswith(".gz") else output_file + ".tmp"
    
    ordered_info = sorted(zip(files, names, requires), key=lambda x: -x[2])
    
    # Pre-resolve count columns from first batch to ensure consistent Int64 schema
    sample_count_cols = [f"Uncon_{n}" for n in names] + [f"Depth_{n}" for n in names]

    for chrom_batch in batches:
        logging.info(f"Processing batch: {chrom_batch[0]}... ({len(chrom_batch)} chroms)")
        
        # Build one giant lazy plan for the entire batch
        df_batch_lazy = None
        shinked = False
        
        for f, n, r in ordered_info:
            is_countmut = file_formats[n]
            lf_sample = get_lazy_plan(f, n, is_countmut).filter(pl.col("Chrom").is_in(chrom_batch))
            
            # Cast to Int64 early to avoid diagonal concat issues
            lf_sample = lf_sample.with_columns([pl.col(c).cast(pl.Int64) for c in lf_sample.collect_schema().names() if c.startswith(("Uncon_", "Depth_"))])

            if df_batch_lazy is None:
                df_batch_lazy = lf_sample
            else:
                df_batch_lazy = df_batch_lazy.join(
                    lf_sample, 
                    on=["Chrom", "Pos", "Strand"], 
                    how="full" if r else "left", 
                    coalesce=True, 
                    suffix="_right"
                ).with_columns(
                    pl.when(pl.col("Motif").is_null()).then(pl.col("Motif_right")).otherwise(pl.col("Motif")).alias("Motif")
                ).drop("Motif_right")
                
                # Apply intermediate filter if we have optional samples coming up
                if not r and not shinked:
                    df_batch_lazy = df_batch_lazy.filter(pl.max_horizontal(pl.col("^Depth_.*$")) >= min_depth)
                    shinked = True

        # Execute the entire join for this batch in streaming mode
        if df_batch_lazy is not None:
            df_res = df_batch_lazy.fill_null(0).sort(["Chrom", "Pos", "Strand"]).collect(engine="streaming")
            
            if not df_res.is_empty():
                with open(temp_output, "ab") as f_out:
                    df_res.write_csv(f_out, separator="\t", include_header=first_batch, quote_style="never")
                first_batch = False
            
            del df_res
            import gc; gc.collect()

    if not first_batch and output_file.endswith(".gz"):
        logging.info(f"Compressing to {output_file}...")
        # Use system gzip for speed
        subprocess.run(["gzip", "-f", temp_output])
        if temp_output != output_file:
            os.rename(temp_output + ".gz", output_file)
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
