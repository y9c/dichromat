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
    parser.add_argument("--motif-files", nargs="+")
    parser.add_argument("--sites-file")
    
    args = parser.parse_args()

    # 1. Motif Heatmap
    all_dfs = []
    if args.motif_files:
        for f in args.motif_files:
            sample = os.path.basename(f).split('.')[0]
            try:
                df = pl.read_csv(f, separator='\t')
                df = df.with_columns(
                    pl.col('Motif').str.to_uppercase().str.replace_all('T', 'U'),
                    pl.lit(sample).alias('Sample')
                )
                all_dfs.append(df)
            except: pass

    if all_dfs:
        df_concat = pl.concat(all_dfs)
        df_pivot = df_concat.pivot(on='Sample', index='Motif', values='Ratio').sort("Motif")
        header = [
            "# id: motif_conversion_heatmap",
            "# section_name: 'Motif Conversion Ratios'",
            "# description: 'Global conversion ratio (Unconverted / Depth) for all 3-mer motifs.'",
            "# plot_type: 'heatmap'",
            "# pconfig: {title: 'Motif Ratios', x_title: 'Sample', y_title: 'Motif', min: 0, max: 1, stops: [[0, '#f7fcf0'], [0.5, '#7bccc4'], [1, '#084081']]}",
        ]
        with open(args.motif_output, 'w') as f_out:
            f_out.write("\n".join(header) + "\n")
            df_pivot.write_csv(f_out, separator='\t', include_header=True)

    # 2. Site-level Distribution (Coverage & Ratio)
    if args.sites_file and os.path.exists(args.sites_file):
        print(f"Processing sites file: {args.sites_file}")
        try:
            df_sites = pl.read_csv(args.sites_file, separator='\t', infer_schema_length=None)
            print(f"Loaded {len(df_sites)} sites")
            
            # Identify Depth and Uncon columns
            depth_cols = [c for c in df_sites.columns if c.startswith("Depth_")]
            print(f"Found depth columns: {depth_cols}")
            
            summary_data = []
            dist_data = []
            
            bins = np.linspace(0, 1, 21)
            bin_labels = [f"{bins[i]:.2f}-{bins[i+1]:.2f}" for i in range(len(bins)-1)]
            
            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                print(f"Processing sample: {sample}")
                
                # Filter for depth > 0, handle potential nulls
                valid_df = df_sites.filter(pl.col(d_col).is_not_null())
                valid_df = valid_df.filter(pl.col(d_col).cast(pl.Int64) > 0)
                
                if valid_df.is_empty(): 
                    print(f"No valid sites for {sample}")
                    continue
                
                depths = valid_df[d_col].cast(pl.Int64)
                uncons = valid_df[u_col].cast(pl.Int64)
                ratios = uncons / depths
                
                print(f"Found {len(valid_df)} valid sites for {sample}")
                
                # --- Summary ---
                summary_data.append({
                    'Sample': sample,
                    'Total_Sites': len(valid_df),
                    'Mean_Depth': float(depths.mean()),
                    'Median_Depth': float(depths.median()),
                    'Mean_Ratio': float(ratios.mean()),
                    'Max_Ratio': float(ratios.max())
                })
                
                # --- Distribution ---
                counts, _ = np.histogram(ratios.to_numpy(), bins=bins)
                row = {'Sample': sample}
                for label, count in zip(bin_labels, counts):
                    row[label] = int(count)
                dist_data.append(row)
            
            if summary_data:
                print(f"Writing summary to {args.summary_output}")
                df_summary = pl.DataFrame(summary_data)
                header_sum = ["# id: site_summary_table", "# section_name: 'Site Calling Summary'", "# description: 'General statistics for detected sites across samples.'", "# plot_type: 'table'", "# pconfig: {namespace: 'Sites', format: '{:,.2f}'}"]
                with open(args.summary_output, 'w') as f_out:
                    f_out.write("\n".join(header_sum) + "\n")
                    df_summary.write_csv(f_out, separator='\t', include_header=True)

            if dist_data:
                print(f"Writing distribution to {args.dist_output}")
                df_dist = pl.DataFrame(dist_data)
                header_dist = ["# id: site_ratio_dist", "# section_name: 'Site Conversion Ratio Distribution'", "# description: 'Distribution of conversion ratios across all detected sites.'", "# plot_type: 'bargraph'", "# pconfig: {id: 'site_ratio_hist', title: 'Conversion Ratio Distribution', ylab: 'Frequency', xlab: 'Ratio Bin'}"]
                with open(args.dist_output, 'w') as f_out:
                    f_out.write("\n".join(header_dist) + "\n")
                    df_dist.write_csv(f_out, separator='\t', include_header=True)
                    
        except Exception as e:
            print(f"Error processing sites file: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
