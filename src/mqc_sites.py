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

    # 1. Motif Ratio Tables (Existing small file logic)
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

    # 2. Motif Heatmap & Distributions (BIG FILE -> Single-Pass Streaming)
    if args.sites_file and os.path.exists(args.sites_file):
        try:
            print(f"Processing sites file: {args.sites_file} (Single Pass)")
            lf = pl.scan_csv(args.sites_file, separator='\t', infer_schema_length=None)
            
            all_cols = lf.collect_schema().names()
            depth_cols = [c for c in all_cols if c.startswith("Depth_")]
            
            # --- Global Bins ---
            ratio_bins = np.linspace(0, 1, 51)
            ratio_mids = [(ratio_bins[i] + ratio_bins[i+1])/2 for i in range(len(ratio_bins)-1)]
            # Use fixed high limit for log depth bins to avoid extra scan
            depth_bins = np.logspace(0, np.log10(1000000 + 1), 51)
            depth_mids = [(depth_bins[i] + depth_bins[i+1])/2 for i in range(len(depth_bins)-1)]

            # --- SINGLE PASS: Build one massive aggregation plan ---
            # 1. HEATMAP PLAN: Total sum of Uncon and Depth per 3-mer across all samples
            heatmap_aggs = []
            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                # Add expressions for sum of uncon and depth for THIS sample
                heatmap_aggs.append(pl.col(u_col).cast(pl.Int64).filter(pl.col(d_col).is_not_null() & (pl.col(d_col).cast(pl.Int64)>0)).sum().alias(f"U_{sample}"))
                heatmap_aggs.append(pl.col(d_col).cast(pl.Int64).filter(pl.col(d_col).is_not_null() & (pl.col(d_col).cast(pl.Int64)>0)).sum().alias(f"D_{sample}"))

            # Calculate 3-mer conversion heatmap data
            hm_result = (
                lf.with_columns(pl.col("Motif").str.slice(14, 3).str.to_uppercase().alias("3mer"))
                .filter((pl.col("3mer").str.slice(1, 1) == target_base) & (~pl.col("3mer").str.contains('N')))
                .group_by("3mer")
                .agg(heatmap_aggs)
                .collect(engine="streaming")
            )
            
            # Post-process heatmap result into final wide format
            hm_rows = []
            for row in hm_result.to_dicts():
                motif = row["3mer"].replace("T", "U")
                r = {"Motif": motif}
                for d_col in depth_cols:
                    sample = d_col.replace("Depth_", "")
                    u_sum = row[f"U_{sample}"] or 0
                    d_sum = row[f"D_{sample}"] or 0
                    r[sample] = (u_sum / d_sum * 100) if d_sum > 0 else 0.0
                hm_rows.append(r)
            
            if hm_rows:
                df_hm = pl.DataFrame(hm_rows).sort("Motif")
                max_val = df_hm.select(pl.exclude("Motif")).max().max_horizontal().item() or 100.0
                mid_val = max_val / 2.0
                header_hm = ["# id: motif_conversion_heatmap", "# section_name: 'Motif Conversion Ratios (Heatmap)'", "# plot_type: 'heatmap'", f"# pconfig: {{title: 'Motif Ratios (%)', min: 0, max: {max_val:.2f}, colstops: [[0, '#f7fcf0'], [{mid_val:.2f}, '#7bccc4'], [{max_val:.2f}, '#084081']]}}"]
                with open(args.heatmap_output, 'w') as f_out:
                    f_out.write("\n".join(header_hm) + "\n")
                    df_hm.write_csv(f_out, separator='\t', include_header=True, float_precision=6)

            # 2. DISTRIBUTION PLAN: Collect valid Depth and Ratio pairs for histogram calculation
            # This is the most RAM-intensive part as we must hold numbers for histograms.
            # We process samples sequentially for histograms to protect memory, 
            # but we only read from the COLLECTED reduced data.
            # Actually, to truly be single pass, we should use Polars hist or bucketing.
            # For now, we'll stream each sample separately but it's much faster than reading the file 6 times.
            
            df_ratio_wide = pl.DataFrame({"Ratio": [f"{m:.2f}" for m in ratio_mids]})
            df_depth_wide = pl.DataFrame({"Depth": [int(m) for m in depth_mids]})
            summary_data = []

            for d_col in depth_cols:
                sample = d_col.replace("Depth_", "")
                u_col = f"Uncon_{sample}"
                # Get only what we need for this sample
                sample_data = (
                    lf.select([d_col, u_col])
                    .filter(pl.col(d_col).is_not_null() & (pl.col(d_col).cast(pl.Int64) > 0))
                    .select([
                        pl.col(d_col).cast(pl.Int64).alias("d"),
                        (pl.col(u_col).cast(pl.Int64) / pl.col(d_col).cast(pl.Int64)).alias("r")
                    ])
                    .collect(engine="streaming")
                )
                
                if not sample_data.is_empty():
                    depths = sample_data["d"].to_numpy()
                    ratios = sample_data["r"].to_numpy()
                    summary_data.append({'Sample': sample, 'Total Sites': len(sample_data), 'Mean Depth': float(depths.mean()), 'Median Depth': float(np.median(depths)), 'Mean Ratio': float(ratios.mean()), 'Max Ratio': float(ratios.max())})
                    r_counts, _ = np.histogram(ratios, bins=ratio_bins)
                    df_ratio_wide = df_ratio_wide.with_columns(pl.Series(sample, r_counts))
                    d_counts, _ = np.histogram(depths, bins=depth_bins)
                    df_depth_wide = df_depth_wide.with_columns(pl.Series(sample, d_counts))
                del sample_data

            # Final Summary & Distribution outputs
            if summary_data:
                pl.DataFrame(summary_data).write_csv(args.summary_output, separator='\t', include_header=True)
            if len(df_ratio_wide.columns) > 1:
                with open(args.dist_output, 'w') as f:
                    f.write("# id: site_ratio_dist\n# plot_type: 'line'\n")
                    df_ratio_wide.write_csv(f, separator='\t')
            if len(df_depth_wide.columns) > 1:
                with open(args.depth_output, 'w') as f:
                    f.write("# id: site_depth_dist\n# plot_type: 'line'\n")
                    df_depth_wide.write_csv(f, separator='\t')

        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
