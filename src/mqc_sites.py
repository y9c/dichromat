#!/usr/bin/env python
import sys
import polars as pl
import os
import argparse
import numpy as np
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("heatmap_output")
    parser.add_argument("summary_output")
    parser.add_argument("dist_output")
    parser.add_argument("depth_output")
    parser.add_argument("transcript_table_output")
    parser.add_argument("genome_table_output")
    parser.add_argument("--motif-files", nargs="+")
    parser.add_argument("--sites-file", nargs="+", help="Input sites files (Parquet or TSV)")
    parser.add_argument("--target-base", default="A")
    args = parser.parse_args()

    if not args.sites_file:
        logging.error("No sites file provided.")
        return

    try:
        logging.info(f"Scanning input files: {args.sites_file}")
        lfs = []
        for f in args.sites_file:
            if f.endswith(".parquet"):
                lfs.append(pl.scan_parquet(f))
            else:
                lfs.append(pl.scan_csv(f, separator='\t', infer_schema_length=None))
        
        lf = pl.concat(lfs)
        schema = lf.collect_schema()
        depth_cols = sorted([c for c in schema.names() if c.startswith("Depth_")])
        libraries = [c.replace("Depth_", "") for c in depth_cols]
        
        if not libraries:
            logging.warning("No depth columns found in sites file.")
            return

        # 1. MOTIF HEATMAP
        if args.motif_files:
            logging.info("Processing motif conversion files...")
            motif_dfs = []
            for f in args.motif_files:
                sample_name = os.path.basename(f).split(".")[0]
                reftype = "transcript" if "transcript" in f else "genome"
                df = pl.read_csv(f, separator='\t')
                if not df.is_empty():
                    df = df.with_columns([
                        pl.lit(sample_name).alias("sample"),
                        pl.lit(reftype).alias("reftype")
                    ])
                    motif_dfs.append(df)
            
            if motif_dfs:
                df_motifs = pl.concat(motif_dfs)
                df_agg = df_motifs.group_by(["Motif", "reftype"]).agg([
                    (pl.col("Unconverted").sum() / pl.col("Depth").sum()).alias("Ratio")
                ])
                
                for reftype in ["transcript", "genome"]:
                    df_p = df_agg.filter(pl.col("reftype") == reftype).pivot(
                        on="Motif", index="reftype", values="Ratio"
                    )
                    if not df_p.is_empty():
                        df_p = df_p.drop("reftype")
                        out_path = args.transcript_table_output if reftype == "transcript" else args.genome_table_output
                        df_p.write_csv(out_path, separator='\t')

        # 2. GLOBAL STREAMING AGGREGATION
        logging.info("Executing global streaming aggregation for Summary and Histograms...")
        
        ratio_bins = np.linspace(0, 1, 51)
        depth_bins = np.logspace(0, 5, 51)
        
        summary_exprs = []
        for lib in libraries:
            d_col = f"Depth_{lib}"
            u_col = f"Uncon_{lib}"
            summary_exprs.extend([
                pl.col(d_col).count().alias(f"cnt_{lib}"),
                pl.col(d_col).sum().alias(f"sum_d_{lib}"),
                (pl.col(u_col).cast(pl.Float64) / pl.col(d_col).cast(pl.Float64)).sum().alias(f"sum_r_{lib}"),
                (pl.col(u_col).cast(pl.Float64) / pl.col(d_col).cast(pl.Float64)).max().alias(f"max_r_{lib}")
            ])

        df_summary_raw = lf.select(summary_exprs).collect(engine="streaming")
        
        summary_rows = []
        for lib in libraries:
            cnt = df_summary_raw[0, f"cnt_{lib}"]
            if cnt > 0:
                summary_rows.append({
                    "Sample": lib,
                    "Total Sites": cnt,
                    "Mean Depth": df_summary_raw[0, f"sum_d_{lib}"] / cnt,
                    "Mean Ratio": df_summary_raw[0, f"sum_r_{lib}"] / cnt,
                    "Max Ratio": df_summary_raw[0, f"max_r_{lib}"]
                })
        
        if summary_rows:
            pl.DataFrame(summary_rows).write_csv(args.summary_output, separator='\t')

        # 3. HISTOGRAMS
        logging.info("Generating histograms...")
        df_ratio_hist = pl.DataFrame({"Ratio": [f"{m:.2f}" for m in (ratio_bins[:-1] + ratio_bins[1:]) / 2]})
        df_depth_hist = pl.DataFrame({"Depth": [int(m) for m in (depth_bins[:-1] + depth_bins[1:]) / 2]})

        lib_groups = [libraries[i:i+4] for i in range(0, len(libraries), 4)]
        for group in lib_groups:
            logging.info(f"  Processing group: {group}")
            group_cols = []
            for lib in group:
                group_cols.extend([f"Depth_{lib}", f"Uncon_{lib}"])
            
            df_group = lf.select(group_cols).collect(engine="streaming")
            
            for lib in group:
                d_vals = df_group[f"Depth_{lib}"].filter(df_group[f"Depth_{lib}"] > 0).to_numpy()
                u_vals = df_group[f"Uncon_{lib}"].filter(df_group[f"Depth_{lib}"] > 0).to_numpy()
                if len(d_vals) > 0:
                    r_vals = u_vals / d_vals
                    r_c, _ = np.histogram(r_vals, bins=ratio_bins)
                    d_c, _ = np.histogram(d_vals, bins=depth_bins)
                    df_ratio_hist = df_ratio_hist.with_columns(pl.Series(lib, r_c))
                    df_depth_hist = df_depth_hist.with_columns(pl.Series(lib, d_c))
            del df_group

        df_ratio_hist.write_csv(args.dist_output, separator='\t')
        df_depth_hist.write_csv(args.depth_output, separator='\t')
        
        logging.info("MQC Site Aggregation Complete.")

    except Exception as e:
        logging.error(f"Error in mqc_sites: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
