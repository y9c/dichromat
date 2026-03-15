#!/usr/bin/env python
import logging
import os
import polars as pl
import re
import argparse
import glob
from pathlib import Path

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
                    "GeneName": gene_id,
                    "Chrom": chrom,
                    "Strand": strand,
                    "g_start": g_start,
                    "g_end": g_end,
                    "tx_start": tx_start,
                    "tx_end": tx_end,
                })
    return pl.DataFrame(
        records,
        schema={
            "GeneName": pl.String,
            "Chrom": pl.String,
            "Strand": pl.String,
            "g_start": pl.Int64,
            "g_end": pl.Int64,
            "tx_start": pl.Int64,
            "tx_end": pl.Int64,
        },
    )


def scan_input(f):
    """Scan Parquet or TSV.GZ safely."""
    if f.endswith(".parquet"):
        return pl.scan_parquet(f)
    else:
        return pl.scan_csv(f, separator="\t", infer_schema_length=None)


def remap_and_join_files_parquet(gene_df_file, genome_df_file, transcript_file, output_file, min_depth=5):
    pl.enable_string_cache()

    logging.info("Step 1: Parsing transcript mapping file")
    gene_order_list = []
    seen_genes = set()
    with open(transcript_file, "r") as f:
        header = f.readline()
        for line in f:
            gid = line.split("\t")[0]
            if gid not in seen_genes:
                gene_order_list.append(gid)
                seen_genes.add(gid)

    exons_df_raw = parse_tx_file_to_df(transcript_file)
    gene_map_df = pl.DataFrame(
        {"GeneName": gene_order_list, "GeneIdx": pl.Series(range(len(gene_order_list)), dtype=pl.Int32)}
    )
    exons_df_all = exons_df_raw.join(gene_map_df, on="GeneName").drop("GeneName")
    gene_map_df_lazy = gene_map_df.lazy()

    logging.info("Step 2: Identifying chromosomes for batching")
    lf_genome = scan_input(genome_df_file)
    all_chroms = lf_genome.select("Chrom").unique().collect().get_column("Chrom").to_list()

    main_chroms = sorted([c for c in all_chroms if re.match(r"^(chr)?([0-9]+|[XYM]|MT)$", c)])
    other_contigs = [c for c in all_chroms if c not in main_chroms]

    batches = []
    batch_size = 5
    for i in range(0, len(main_chroms), batch_size):
        batches.append(main_chroms[i : i + batch_size])
    if other_contigs:
        batches.append(other_contigs)

    logging.info(f"Processing in {len(batches)} coarse batches (OOM-Safe)")

    first_pass = True
    count_cols = []
    out_dir = Path(output_file).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_prefix = str(out_dir / (Path(output_file).name + ".tmp_chunk"))

    gene_names_list = gene_order_list

    chunk_files = []
    for idx, chrom_batch in enumerate(batches):
        logging.info(f"Processing batch {idx+1}: {chrom_batch[0]}... ({len(chrom_batch)} chroms)")

        exons_batch = exons_df_all.filter(pl.col("Chrom").is_in(chrom_batch)).lazy()
        relevant_genes_list = (
            exons_df_raw.filter(pl.col("Chrom").is_in(chrom_batch)).get_column("GeneName").unique().to_list()
        )

        # 1. Transcript remapping
        df1_lazy = (
            scan_input(gene_df_file)
            .filter(pl.col("Chrom").is_in(relevant_genes_list))
            .rename({"Chrom": "GeneName", "Pos": "GenePos_1based"})
            .drop(["Strand"])
            .with_columns((pl.col("GenePos_1based") - 1).alias("GenePos_0based"))
            .join(gene_map_df_lazy, on="GeneName", how="inner")
            .join(exons_batch, on="GeneIdx", how="inner")
            .filter(
                ((pl.col("GenePos_1based") - 1 >= pl.col("tx_start")) & (pl.col("GenePos_1based") - 1 < pl.col("tx_end")))
            )
            .with_columns(
                Pos=pl.when(pl.col("Strand") == "+")
                .then(pl.col("g_start") + (pl.col("GenePos_1based") - 1 - pl.col("tx_start")) + 1)
                .otherwise(pl.col("g_end") - (pl.col("GenePos_1based") - 1 - pl.col("tx_start"))),
                GenePos=pl.col("GenePos_1based").cast(pl.UInt32),
            )
            .with_columns(
                [pl.col("Chrom").fill_null(pl.col("GeneName")), pl.col("Pos").cast(pl.UInt32), pl.col("Strand").fill_null(".")]
            )
        )

        # 2. Genome data
        df2_lazy = (
            scan_input(genome_df_file)
            .filter(pl.col("Chrom").is_in(chrom_batch))
            .with_columns(
                [
                    pl.col("Pos").cast(pl.UInt32),
                    pl.lit(None).cast(pl.Int32).alias("GeneIdx"),
                    pl.lit(None).cast(pl.UInt32).alias("GenePos"),
                ]
            )
        )

        if first_pass:
            schema = df1_lazy.collect_schema()
            count_cols = [c for c in schema.names() if c.startswith(("Uncon_", "Depth_"))]
            first_pass = False

        required_cols = ["Chrom", "Pos", "Strand", "GeneIdx", "GenePos", "Motif"] + count_cols

        df_chunk = (
            pl.concat(
                [
                    df1_lazy.select(required_cols).with_columns([pl.col(c).cast(pl.Int64) for c in count_cols]),
                    df2_lazy.select(required_cols).with_columns([pl.col(c).cast(pl.Int64) for c in count_cols]),
                ],
                how="diagonal",
            )
            .with_columns(
                [pl.col("Chrom").cast(pl.Categorical), pl.col("Strand").cast(pl.Categorical), pl.col("Motif").cast(pl.Categorical)]
            )
            .group_by(["Chrom", "Pos", "Strand"])
            .agg(
                [
                    pl.col("GeneIdx").drop_nulls().alias("ids"),
                    pl.col("GenePos").drop_nulls().alias("pos"),
                    pl.col("Motif").sort().first(),
                    pl.exclude(["Chrom", "Pos", "Strand", "GeneIdx", "GenePos", "Motif", "ids", "pos"]).sum(),
                ]
            )
            .filter(pl.sum_horizontal(pl.col("^Depth_.*$")) >= min_depth)
            .collect(engine="streaming")
        )

        if not df_chunk.is_empty():

            def format_mapped(row, names_list):
                ids, pos = row["ids"], row["pos"]
                if ids is None or len(ids) == 0:
                    return '""', '""'
                seen, final_names, final_pos = set(), [], []
                for i, p in zip(ids, pos):
                    if i not in seen:
                        final_names.append(names_list[i])
                        final_pos.append(str(p))
                        seen.add(i)
                linked = sorted(zip(list(seen), final_names, final_pos), key=lambda x: x[0])
                return ";".join([x[1] for x in linked]), ";".join([x[2] for x in linked])

            df_chunk = (
                df_chunk.with_columns(
                    pl.struct(["ids", "pos"])
                    .map_elements(
                        lambda x: format_mapped(x, gene_names_list),
                        return_dtype=pl.Struct([pl.Field("GeneName", pl.String), pl.Field("GenePos", pl.String)]),
                    )
                    .alias("fmt")
                )
                .unnest("fmt")
                .drop(["ids", "pos"])
                .select(["Chrom", "Pos", "Strand", "GeneName", "GenePos", "Motif"] + count_cols)
                .sort(["Chrom", "Pos", "Strand"])
            )
            chunk_file = f"{temp_prefix}.{idx}.parquet"
            df_chunk.write_parquet(chunk_file, compression="zstd")
            chunk_files.append(chunk_file)

        import gc

        gc.collect()

    # Final Merge of Parquet Chunks
    logging.info("Merging temporary Parquet chunks...")
    if chunk_files:
        # Use lazy scan and sink to keep final merge memory-safe
        lf_final = pl.concat([pl.scan_parquet(c) for c in chunk_files]).sort(["Chrom", "Pos", "Strand"])
        if output_file.endswith(".parquet"):
            lf_final.sink_parquet(output_file, compression="zstd")
        elif output_file.endswith(".gz"):
            # Maintain backward compatibility if requested
            lf_final.collect().write_csv(output_file, separator="\t", quote_style="never", compression="gzip")
        else:
            lf_final.collect().write_csv(output_file, separator="\t", quote_style="never")

        for c in chunk_files:
            os.remove(c)

    logging.info("Remapping complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--transcript-file", required=True)
    parser.add_argument("-a", "--gene-file", required=True)
    parser.add_argument("-b", "--genome-file", required=True)
    parser.add_argument("-o", "--output-file", required=True)
    parser.add_argument("--min-depth", type=int, default=1)
    args = parser.parse_args()
    remap_and_join_files_parquet(
        args.gene_file, args.genome_file, args.transcript_file, args.output_file, args.min_depth
    )
