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

    # 1. Motif Ratio Tables (Small files, keep existing eager logic)
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
                    (pl.col('Ratio') * 100).alias(f"{sample} (%)")
                ])
                if reftype == "transcript": transcript_dfs.append(df)
                elif reftype == "genome": genome_dfs.append(df)
            except: pass

    def write_motif_table(dfs, output_path, title, section_id):
        if not dfs: return
        df_final = dfs[0]
        for df in dfs[1:]: df_final = df_final.join(df, on='Motif', how='full', coalesce=True)
        df_final = df_final.sort("Motif")
        col_config = {col: {"format": "{:.6f}%"} for col in df_final.columns if col != "Motif"}
        header = [f"# id: {section_id}", f"# section_name: '{title}'", "# plot_type: 'table'", "# pconfig:", "#    namespace: 'Motif Ratios'", f"#    col_config: {json.dumps(col_config)}"]
        with open(output_path, 'w') as f_out:
            f_out.write("\n".join(header) + "\n")
            df_final.write_csv(f_out, separator='\t', include_header=True, float_precision=6)

    write_motif_table(transcript_dfs, args.transcript_table_output, "Motif Ratios (Transcriptome)", "motif_ratio_transcript_table")
    write_motif_table(genome_dfs, args.genome_table_output, "Motif Ratios (Genome)", "motif_ratio_genome_table")

    # 2. Motif Heatmap & Distributions (BIG FILE -> Lazy + Streaming)
    if args.sites_file and os.path.exists(args.sites_file):
        try:
            print(f"Processing sites file: {args.sites_file} (Streaming)")
            lf_sites = pl.scan_csv(args.sites_file, separator='\t', infer_schema_length=None)
            
            # Identify samples from columns
            all_cols = lf_sites.collect_schema().names()
            depth_cols = [c for c in all_cols if c.startswith("Depth_")]
            
            heatmap_dfs = []
            summary_data = []
            
            ratio_bins = np.linspace(0, 1, 51)
            ratio_mids = [(ratio_bins[i] + ratio_bins[i+1])/2 for i in range(len(ratio_bins)-1)]
            df_ratio_wide = pl.DataFrame({"Ratio": [f"{m:.2f}" for m in ratio_mids]})
            
            # Dynamic max depth estimation for log-scale bins
            # (We estimate max depth from a sample of data to avoid a full pass if possible, 
            # or just use a fixed high limit for log bins)
            max_depth = 100000 
            depth_bins = np.logspace(0, np.log10(max_depth + 1), 51)
            depth_mids = [(depth_bins[i] + depth_bins[i+1])/2 for i in range(len(depth_bins)-1)]
            df_depth_wide = pl.DataFrame({"Depth": [int(m) for m in depth_mids]})

            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                print(f"  Calculating stats for sample: {sample}")
                
                # --- HEAPMAP Calculation (Streaming) ---
                # We filter for valid sites for THIS sample only to keep memory low
                sample_agg = (
                    lf_sites.select(["Motif", d_col, u_col])
                    .filter(pl.col(d_col).is_not_null() & (pl.col(d_col).cast(pl.Int64) > 0))
                    .with_columns([
                        pl.col("Motif").str.slice(14, 3).str.to_uppercase().alias("3mer"),
                        pl.col(d_col).cast(pl.Int64),
                        pl.col(u_col).cast(pl.Int64)
                    ])
                    .filter((pl.col("3mer").str.slice(1, 1) == target_base) & (~pl.col("3mer").str.contains('N')))
                    .group_by("3mer")
                    .agg([pl.col(u_col).sum().alias("Total_Uncon"), pl.col(d_col).sum().alias("Total_Depth")])
                    .with_columns((pl.col("Total_Uncon") / pl.col("Total_Depth") * 100).alias(sample))
                    .select([pl.col("3mer").str.replace_all("T", "U").alias("Motif"), sample])
                    .collect(engine="streaming")
                )
                heatmap_dfs.append(sample_agg)

                # --- DISTRIBUTIONS (Requires collecting site values for histograms) ---
                # Even for distributions, we only collect the two required columns for THIS sample
                dist_data = (
                    lf_sites.select([d_col, u_col])
                    .filter(pl.col(d_col).is_not_null() & (pl.col(d_col).cast(pl.Int64) > 0))
                    .with_columns([
                        pl.col(d_col).cast(pl.Int64).alias("d"),
                        (pl.col(u_col).cast(pl.Int64) / pl.col(d_col).cast(pl.Int64)).alias("r")
                    ])
                    .select(["d", "r"])
                    .collect(engine="streaming")
                )
                
                if not dist_data.is_empty():
                    depths = dist_data["d"].to_numpy()
                    ratios = dist_data["r"].to_numpy()
                    
                    summary_data.append({
                        'Sample': sample, 
                        'Total Sites': len(dist_data), 
                        'Mean Depth': float(depths.mean()), 
                        'Median Depth': float(np.median(depths)), 
                        'Mean Ratio': float(ratios.mean()), 
                        'Max Ratio': float(ratios.max())
                    })
                    
                    r_counts, _ = np.histogram(ratios, bins=ratio_bins)
                    df_ratio_wide = df_ratio_wide.with_columns(pl.Series(sample, r_counts))
                    d_counts, _ = np.histogram(depths, bins=depth_bins)
                    df_depth_wide = df_depth_wide.with_columns(pl.Series(sample, d_counts))
                
                del dist_data # Free memory for next sample
            
            # --- WRITE OUTPUTS ---
            if heatmap_dfs:
                df_hm = heatmap_dfs[0]
                for df in heatmap_dfs[1:]: df_hm = df_hm.join(df, on="Motif", how="full", coalesce=True)
                df_hm = df_hm.sort("Motif")
                numeric_cols = [c for c in df_hm.columns if c != "Motif"]
                max_val = df_hm.select(numeric_cols).max().max_horizontal().item() or 100.0
                mid_val = max_val / 2.0
                header_hm = ["# id: motif_conversion_heatmap", "# section_name: 'Motif Conversion Ratios (Heatmap)'", "# plot_type: 'heatmap'", f"# pconfig: {{title: 'Motif Ratios (%)', min: 0, max: {max_val:.2f}, colstops: [[0, '#f7fcf0'], [{mid_val:.2f}, '#7bccc4'], [{max_val:.2f}, '#084081']]}}"]
                with open(args.heatmap_output, 'w') as f_out:
                    f_out.write("\n".join(header_hm) + "\n")
                    df_hm.write_csv(f_out, separator='\t', include_header=True, float_precision=6)

            if summary_data:
                df_summary = pl.DataFrame(summary_data)
                header_sum = ["# id: site_summary_table", "# section_name: 'Site Calling Summary'", "# plot_type: 'table'", "# pconfig: {namespace: 'Sites', format: '{:,.2f}'}"]
                with open(args.summary_output, 'w') as f_out:
                    f_out.write("\n".join(header_sum) + "\n")
                    df_summary.write_csv(f_out, separator='\t', include_header=True)

            if len(df_ratio_wide.columns) > 1:
                header_ratio = ["# id: site_ratio_dist", "# section_name: 'Site Conversion Ratio Distribution'", "# plot_type: 'line'", "# pconfig: {title: 'Site Conversion Ratios', xlab: 'Conversion Ratio', ylab: 'Number of Sites', categories: true, smooth_points: false}"]
                with open(args.dist_output, 'w') as f_out:
                    f_out.write("\n".join(header_ratio) + "\n")
                    df_ratio_wide.write_csv(f_out, separator='\t', include_header=True)

            if len(df_depth_wide.columns) > 1:
                header_depth = ["# id: site_depth_dist", "# section_name: 'Site Depth Distribution'", "# plot_type: 'line'", "# pconfig: {title: 'Site Coverage Depth', xlab: 'Depth (Reads)', ylab: 'Number of Sites', xlog: true, categories: true, smooth_points: false}"]
                with open(args.depth_output, 'w') as f_out:
                    f_out.write("\n".join(header_depth) + "\n")
                    df_depth_wide.write_csv(f_out, separator='\t', include_header=True)
                    
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
