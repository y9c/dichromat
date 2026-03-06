#!/usr/bin/env python
import sys
import polars as pl
import os
import argparse
import numpy as np

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("motif_output")
    parser.add_argument("summary_output")
    parser.add_argument("dist_output")
    parser.add_argument("depth_output")
    parser.add_argument("--motif-files", nargs="+")
    parser.add_argument("--sites-file")
    
    args = parser.parse_args()

    # 1. Motif Heatmap (High precision %, dynamic scale)
    all_dfs = []
    if args.motif_files:
        for f in args.motif_files:
            name = os.path.basename(f).replace('.tsv', '')
            try:
                df = pl.read_csv(f, separator='\t')
                df = df.with_columns(
                    pl.col('Motif').str.to_uppercase().str.replace_all('T', 'U'),
                    (pl.col('Ratio') * 100).alias('Ratio_Pct'),
                    pl.lit(name).alias('Sample')
                )
                all_dfs.append(df)
            except: pass

    if all_dfs:
        df_concat = pl.concat(all_dfs)
        df_pivot = df_concat.pivot(on='Sample', index='Motif', values='Ratio_Pct').sort("Motif")
        
        numeric_cols = [c for c in df_pivot.columns if c != "Motif"]
        max_val = 0
        if numeric_cols:
            max_val = df_pivot.select(numeric_cols).max().max_horizontal().item()
        
        if max_val is None or max_val == 0:
            max_val = 100.0
        
        mid_val = max_val / 2.0
        
        header = [
            "# id: motif_conversion_heatmap",
            "# section_name: 'Motif Conversion Ratios'",
            "# description: 'Global conversion ratio (Unconverted / Depth) for all 3-mer motifs, shown in %.'",
            "# plot_type: 'heatmap'",
            f"# pconfig: {{title: 'Motif Ratios (%)', min: 0, max: {max_val:.2f}, colstops: [[0, '#f7fcf0'], [{mid_val:.2f}, '#7bccc4'], [{max_val:.2f}, '#084081']]}}",
        ]
        with open(args.motif_output, 'w') as f_out:
            f_out.write("\n".join(header) + "\n")
            df_pivot.write_csv(f_out, separator='\t', include_header=True, float_precision=4)

    # 2. Site-level Distribution
    if args.sites_file and os.path.exists(args.sites_file):
        try:
            df_sites = pl.read_csv(args.sites_file, separator='\t', infer_schema_length=None)
            depth_cols = [c for c in df_sites.columns if c.startswith("Depth_")]
            
            summary_data = []
            ratio_dist_dfs = []
            depth_dist_dfs = []
            
            # Bins for Ratio (50 bins)
            ratio_bins = np.linspace(0, 1, 51)
            ratio_labels = [f"{ (ratio_bins[i] + ratio_bins[i+1])/2 :.2f}" for i in range(len(ratio_bins)-1)]
            
            # Bins for Depth (log scale, 50 bins)
            max_depth = 1000
            for d_col in depth_cols:
                col_max = df_sites.select(pl.col(d_col).cast(pl.Int64)).max().item()
                if col_max and col_max > max_depth: max_depth = col_max
            
            depth_bins = np.logspace(0, np.log10(max_depth + 1), 51)
            depth_labels = [f"{int((depth_bins[i] + depth_bins[i+1])/2)}" for i in range(len(depth_bins)-1)]
            
            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                
                valid_df = df_sites.filter(pl.col(d_col).is_not_null())
                valid_df = valid_df.filter(pl.col(d_col).cast(pl.Int64) > 0)
                
                if valid_df.is_empty(): continue
                
                depths = valid_df[d_col].cast(pl.Int64).to_numpy()
                uncons = valid_df[u_col].cast(pl.Int64).to_numpy()
                ratios = uncons / depths
                
                # --- Summary ---
                summary_data.append({
                    'Sample': sample,
                    'Total_Sites': len(valid_df),
                    'Mean_Depth': float(depths.mean()),
                    'Median_Depth': float(np.median(depths)),
                    'Mean_Ratio': float(ratios.mean()),
                    'Max_Ratio': float(ratios.max())
                })
                
                # --- Ratio Distribution ---
                r_counts, _ = np.histogram(ratios, bins=ratio_bins)
                r_row = {'Sample': sample}
                for label, count in zip(ratio_labels, r_counts):
                    r_row[label] = int(count)
                ratio_dist_dfs.append(pl.DataFrame([r_row]))

                # --- Depth Distribution ---
                d_counts, _ = np.histogram(depths, bins=depth_bins)
                d_row = {'Sample': sample}
                for label, count in zip(depth_labels, d_counts):
                    d_row[label] = int(count)
                depth_dist_dfs.append(pl.DataFrame([d_row]))
            
            if summary_data:
                df_summary = pl.DataFrame(summary_data)
                header_sum = ["# id: site_summary_table", "# section_name: 'Site Calling Summary'", "# plot_type: 'table'", "# pconfig: {namespace: 'Sites', format: '{:,.2f}'}"]
                with open(args.summary_output, 'w') as f_out:
                    f_out.write("\n".join(header_sum) + "\n")
                    df_summary.write_csv(f_out, separator='\t', include_header=True)

            if ratio_dist_dfs:
                df_ratio_dist = pl.concat(ratio_dist_dfs)
                header_ratio = [
                    "# id: site_ratio_dist", 
                    "# section_name: 'Site Conversion Ratio Distribution'", 
                    "# plot_type: 'line'", 
                    "# pconfig:",
                    "#    id: 'site_ratio_lineplot'",
                    "#    title: 'Site Conversion Ratios'",
                    "#    ylab: 'Number of Sites'",
                    "#    xlab: 'Conversion Ratio'",
                    "#    categories: true",
                    "#    smooth_points: false"
                ]
                with open(args.dist_output, 'w') as f_out:
                    f_out.write("\n".join(header_ratio) + "\n")
                    df_ratio_dist.write_csv(f_out, separator='\t', include_header=True)

            if depth_dist_dfs:
                df_depth_dist = pl.concat(depth_dist_dfs)
                header_depth = [
                    "# id: site_depth_dist", 
                    "# section_name: 'Site Depth Distribution'", 
                    "# description: 'Distribution of sequencing depth across all detected sites.'", 
                    "# plot_type: 'line'", 
                    "# pconfig:",
                    "#    id: 'site_depth_lineplot'",
                    "#    title: 'Site Coverage Depth'",
                    "#    ylab: 'Number of Sites'",
                    "#    xlab: 'Depth (Reads)'",
                    "#    xlog: true",
                    "#    categories: true",
                    "#    smooth_points: false"
                ]
                with open(args.depth_output, 'w') as f_out:
                    f_out.write("\n".join(header_depth) + "\n")
                    df_depth_dist.write_csv(f_out, separator='\t', include_header=True)
                    
        except Exception as e:
            print(f"Error processing sites file: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
