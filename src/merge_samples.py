#!/usr/bin/env python
import logging
import os
import polars as pl
import re
import argparse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_file_format(f):
    try:
        if f.endswith(".parquet"):
            return "parquet"
        line = pl.read_csv(f, n_rows=1, has_header=False).item(0, 0)
        return "countmut" if line.startswith("chrom") else "legacy"
    except:
        return "legacy"


def get_lazy_plan(f, n, fmt):
    if fmt == "parquet":
        lf = pl.scan_parquet(f)
        return lf.rename(
            {
                c: f"{c}_{n}"
                for c in ["Uncon", "Depth"]
                if c in lf.collect_schema().names()
            }
        )
    elif fmt == "countmut":
        return (
            pl.scan_csv(
                f, separator="\t", has_header=True, schema_overrides={"chrom": pl.Utf8}
            )
            .select(
                [
                    pl.col("chrom").alias("Chrom"),
                    pl.col("pos").alias("Pos").cast(pl.UInt32),
                    pl.col("strand").alias("Strand"),
                    pl.col("motif").alias("Motif"),
                    (pl.col("u1") + pl.col("u2")).alias(f"Uncon_{n}").cast(pl.UInt32),
                    (pl.col("u1") + pl.col("m1") + pl.col("u2") + pl.col("m2"))
                    .alias(f"Depth_{n}")
                    .cast(pl.UInt32),
                ]
            )
            .filter(pl.col(f"Depth_{n}") > 0)
        )
    else:
        coltypes = {
            "Chrom": pl.Utf8,
            "Pos": pl.UInt32,
            "Strand": pl.Utf8,
            "Motif": pl.Utf8,
            "U0": pl.UInt32,
            "D0": pl.UInt32,
            "U1": pl.UInt32,
            "D1": pl.UInt32,
            "U2": pl.UInt32,
            "D2": pl.UInt32,
        }
        return (
            pl.scan_csv(
                f,
                has_header=False,
                new_columns=list(coltypes.keys()),
                schema_overrides=coltypes,
                separator="\t",
            )
            .filter(pl.col("D1") > 0)
            .rename({"U1": f"Uncon_{n}", "D1": f"Depth_{n}"})
            .select(["Chrom", "Pos", "Strand", "Motif", f"Uncon_{n}", f"Depth_{n}"])
        )


def merge_samples_batched_parquet(files, names, requires, output_file, min_depth=3):
    pl.enable_string_cache()

    logging.info("Identifying unique chromosomes across all samples...")
    all_chroms_set = set()
    file_info = []
    for f, n, r in zip(files, names, requires):
        fmt = get_file_format(f)
        lf = pl.scan_csv(
            f, separator="\t", has_header=(fmt == "countmut"), infer_schema_length=0
        )
        chrom_col = lf.collect_schema().names()[0]
        chroms = (
            lf.select(pl.col(chrom_col).alias("Chrom"))
            .unique()
            .collect()
            .get_column("Chrom")
            .to_list()
        )
        all_chroms_set.update(chroms)
        file_info.append((f, n, r, fmt))

    all_chroms = sorted(list(all_chroms_set))
    main_chroms = sorted(
        [c for c in all_chroms if re.match(r"^(chr)?([0-9]+|[XYM]|MT)$", c)]
    )
    other_contigs = [c for c in all_chroms if c not in main_chroms]

    # Process in batches of 5 to balance speed and RAM
    batches = []
    chunk_size = 5
    for i in range(0, len(main_chroms), chunk_size):
        batches.append(main_chroms[i : i + chunk_size])
    if other_contigs:
        batches.append(other_contigs)

    logging.info(f"Processing in {len(batches)} batches")

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    temp_prefix = output_file.replace(".parquet", "") + ".tmp"

    # Sort files by requiredness
    ordered_info = sorted(file_info, key=lambda x: -x[2])

    chunk_files = []
    for chrom_batch in batches:
        logging.info(f"Processing batch: {chrom_batch[0]}...")
        df_batch_lazy = None
        shinked = False

        for f, n, r, fmt in ordered_info:
            lf_sample = get_lazy_plan(f, n, fmt).filter(
                pl.col("Chrom").is_in(chrom_batch)
            )
            count_cols = [
                c
                for c in lf_sample.collect_schema().names()
                if c.startswith(("Uncon_", "Depth_"))
            ]
            lf_sample = lf_sample.with_columns(
                [pl.col(c).cast(pl.Int64) for c in count_cols]
            )

            if df_batch_lazy is None:
                df_batch_lazy = lf_sample
            else:
                df_batch_lazy = (
                    df_batch_lazy.join(
                        lf_sample,
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
                if not r and not shinked:
                    df_batch_lazy = df_batch_lazy.filter(
                        pl.max_horizontal(pl.col("^Depth_.*$")) >= min_depth
                    )
                    shinked = True

        if df_batch_lazy is not None:
            df_res = (
                df_batch_lazy.fill_null(0)
                .sort(["Chrom", "Pos", "Strand"])
                .collect(engine="streaming")
            )
            if not df_res.is_empty():
                chunk_file = f"{temp_prefix}.{chrom_batch[0]}.parquet"
                df_res.write_parquet(chunk_file, compression="zstd")
                chunk_files.append(chunk_file)
            del df_res
            import gc

            gc.collect()

    logging.info("Final merging of chunks...")
    if chunk_files:
        # Use scan_parquet on all chunks and sink to final file
        pl.concat([pl.scan_parquet(c) for c in chunk_files]).sink_parquet(
            output_file, compression="zstd"
        )
        for c in chunk_files:
            os.remove(c)

    logging.info("Merge complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="*", type=str)
    parser.add_argument("--names", nargs="*", type=str)
    parser.add_argument("--requires", nargs="*", type=int)
    parser.add_argument("--min_depth", type=int, default=3)
    parser.add_argument("--output", type=str)
    args = parser.parse_args()
    merge_samples_batched_parquet(
        args.files, args.names, args.requires, args.output, args.min_depth
    )
