#!/usr/bin/env python
import sys
import polars as pl
import os
import argparse
import numpy as np
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("heatmap_output")
    parser.add_argument("summary_output")
    parser.add_argument("dist_output")
    parser.add_argument("depth_output")
    parser.add_argument("transcript_table_output")
    parser.add_argument("genome_table_output")
    parser.add_argument("--motif-files", nargs="+")
    parser.add_argument("--sites-file")
    parser.add_argument("--target-base", default="A")
    
    args = parser.parse_args()
    target_base = args.target_base.upper()

    # 1. Motif Ratio Tables
    transcript_dfs = []
    genome_dfs = []
    if args.motif_files:
        for f in args.motif_files:
            name_parts = os.path.basename(f).replace('.tsv', '').split('.')
            sample = name_parts[0]
            reftype = name_parts[1] if len(name_parts) > 1 else "unknown"
            try:
                df = pl.read_csv(f, separator='\t')
                df = df.filter(~pl.col('Motif').str.to_uppercase().str.contains('N'))
                df = df.select([
                    pl.col('Motif').str.to_uppercase().str.replace_all('T', 'U'),
                    (pl.col('Ratio') * 100).alias(f"{sample} (%)") # Use % for better precision in default MultiQC table
                ])
                if reftype == "transcript": transcript_dfs.append(df)
                elif reftype == "genome": genome_dfs.append(df)
            except: pass

    def write_motif_table(dfs, output_path, title, section_id):
        if not dfs: return
        df_final = dfs[0]
        for df in dfs[1:]: df_final = df_final.join(df, on='Motif', how='full', coalesce=True)
        df_final = df_final.sort("Motif")
        header = [
            f"# id: {section_id}", 
            f"# section_name: '{title}'", 
            "# plot_type: 'table'", 
            "# pconfig:",
            "#    namespace: 'Motif Ratios'",
            "#    scale: false"
        ]
        with open(output_path, 'w') as f_out:
            f_out.write("\n".join(header) + "\n")
            df_final.write_csv(f_out, separator='\t', include_header=True, float_precision=4)

    write_motif_table(transcript_dfs, args.transcript_table_output, "Motif Ratios (Transcriptome)", "motif_ratio_transcript_table")
    write_motif_table(genome_dfs, args.genome_table_output, "Motif Ratios (Genome)", "motif_ratio_genome_table")

    # 2. Motif Heatmap & Distributions (from merged sites file)
    if args.sites_file and os.path.exists(args.sites_file):
        try:
            print(f"Processing sites file: {args.sites_file}")
            df_sites = pl.read_csv(args.sites_file, separator='\t', infer_schema_length=None)
            depth_cols = [c for c in df_sites.columns if c.startswith("Depth_")]
            
            # --- Heatmap ---
            heatmap_data = []
            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                sub = df_sites.filter(pl.col(d_col).is_not_null())
                sub = sub.with_columns([pl.col(d_col).cast(pl.Int64), pl.col(u_col).cast(pl.Int64)]).filter(pl.col(d_col) > 0)
                if sub.is_empty(): continue
                sub = sub.with_columns(pl.col("Motif").str.slice(14, 3).str.to_uppercase().alias("3mer"))
                sub = sub.filter((pl.col("3mer").str.slice(1, 1) == target_base) & (~pl.col("3mer").str.contains('N')))
                agg = sub.group_by("3mer").agg([pl.col(u_col).sum().alias("Total_Uncon"), pl.col(d_col).sum().alias("Total_Depth")])
                agg = agg.with_columns((pl.col("Total_Uncon") / pl.col("Total_Depth") * 100).alias("Ratio_Pct")).select([
                    pl.col("3mer").str.replace_all("T", "U").alias("Motif"),
                    pl.col("Ratio_Pct").alias(sample)
                ])
                heatmap_data.append(agg)
            
            if heatmap_data:
                df_hm = heatmap_data[0]
                for df in heatmap_data[1:]: df_hm = df_hm.join(df, on="Motif", how="full", coalesce=True)
                df_hm = df_hm.sort("Motif")
                numeric_cols = [c for c in df_hm.columns if c != "Motif"]
                max_val = df_hm.select(numeric_cols).max().max_horizontal().item() or 100.0
                mid_val = max_val / 2.0
                header_hm = ["# id: motif_conversion_heatmap", "# section_name: 'Motif Conversion Ratios (Heatmap)'", "# plot_type: 'heatmap'", f"# pconfig: {{title: 'Motif Ratios (%)', min: 0, max: {max_val:.2f}, colstops: [[0, '#f7fcf0'], [{mid_val:.2f}, '#7bccc4'], [{max_val:.2f}, '#084081']]}}"]
                with open(args.heatmap_output, 'w') as f_out:
                    f_out.write("\n".join(header_hm) + "\n")
                    df_hm.write_csv(f_out, separator='\t', include_header=True, float_precision=4)

            # --- Distributions (Long Format for MultiQC) ---
            summary_data = []
            ratio_bins = np.linspace(0, 1, 51)
            ratio_mids = [(ratio_bins[i] + ratio_bins[i+1])/2 for i in range(len(ratio_bins)-1)]
            
            max_depth = 1000
            for d_col in depth_cols:
                col_max = df_sites.select(pl.col(d_col).cast(pl.Int64)).max().item()
                if col_max and col_max > max_depth: max_depth = col_max
            depth_bins = np.logspace(0, np.log10(max_depth + 1), 51)
            depth_mids = [(depth_bins[i] + depth_bins[i+1])/2 for i in range(len(depth_bins)-1)]
            
            ratio_long = []
            depth_long = []

            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                valid_df = df_sites.filter(pl.col(d_col).is_not_null())
                valid_df = valid_df.filter(pl.col(d_col).cast(pl.Int64) > 0)
                if valid_df.is_empty(): continue
                depths = valid_df[d_col].cast(pl.Int64).to_numpy()
                uncons = valid_df[u_col].cast(pl.Int64).to_numpy()
                ratios = uncons / depths
                
                summary_data.append({'Sample': sample, 'Total Sites': len(valid_df), 'Mean Depth': float(depths.mean()), 'Median Depth': float(np.median(depths)), 'Mean Ratio': float(ratios.mean()), 'Max Ratio': float(ratios.max())})
                
                # Ratio Dist (Long Format)
                r_counts, _ = np.histogram(ratios, bins=ratio_bins)
                for x, y in zip(ratio_mids, r_counts):
                    ratio_long.append({'Sample': sample, 'Ratio': round(float(x), 2), 'Count': int(y)})

                # Depth Dist (Long Format)
                d_counts, _ = np.histogram(depths, bins=depth_bins)
                for x, y in zip(depth_mids, d_counts):
                    depth_long.append({'Sample': sample, 'Depth': int(x), 'Count': int(y)})
            
            if summary_data:
                df_summary = pl.DataFrame(summary_data)
                header_sum = ["# id: site_summary_table", "# section_name: 'Site Calling Summary'", "# plot_type: 'table'", "# pconfig: {namespace: 'Sites', format: '{:,.2f}'}"]
                with open(args.summary_output, 'w') as f_out:
                    f_out.write("\n".join(header_sum) + "\n")
                    df_summary.write_csv(f_out, separator='\t', include_header=True)

            if ratio_long:
                df_ratio = pl.DataFrame(ratio_long)
                header_ratio = ["# id: site_ratio_dist", "# section_name: 'Site Conversion Ratio Distribution'", "# plot_type: 'line'", "# pconfig: {title: 'Site Conversion Ratios', xlab: 'Conversion Ratio', ylab: 'Number of Sites', smooth_points: false}"]
                with open(args.dist_output, 'w') as f_out:
                    f_out.write("\n".join(header_ratio) + "\n")
                    df_ratio.write_csv(f_out, separator='\t', include_header=True)

            if depth_long:
                df_depth = pl.DataFrame(depth_long)
                header_depth = ["# id: site_depth_dist", "# section_name: 'Site Depth Distribution'", "# plot_type: 'line'", "# pconfig: {title: 'Site Coverage Depth', xlab: 'Depth (Reads)', ylab: 'Number of Sites', xlog: true, smooth_points: false}"]
                with open(args.depth_output, 'w') as f_out:
                    f_out.write("\n".join(header_depth) + "\n")
                    df_depth.write_csv(f_out, separator='\t', include_header=True)
                    
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
