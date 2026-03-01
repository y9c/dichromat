#!/usr/bin/env python
import sys
import polars as pl
import os
import argparse
import re
import json

def parse_dedup_log(f):
    """Parse markdup log file for key metrics"""
    basename = os.path.basename(f)
    parts = basename.split('.')
    sample = parts[0]
    reftype = parts[1]
    
    stats = {'Sample': sample, 'Type': reftype}
    try:
        with open(f, 'r') as fh:
            for line in fh:
                line = line.strip()
                if "Total reads processed:" in line:
                    m = re.search(r"processed:\s*([0-9,]+)", line)
                    if m: stats['Total_Reads'] = int(m.group(1).replace(',', ''))
                elif "Duplicates removed:" in line:
                    m = re.search(r"removed:\s*([0-9,]+)", line)
                    if m: stats['Duplicates'] = int(m.group(1).replace(',', ''))
                elif "Total Unique Reads:" in line:
                    m = re.search(r"Unique Reads:\s*([0-9,]+)", line)
                    if m: stats['Unique_Reads'] = int(m.group(1).replace(',', ''))
                elif "Deduplication rate:" in line:
                    m = re.search(r"rate:\s*([\d\.]+)%", line)
                    if m: stats['Duplication_Rate'] = float(m.group(1))
    except Exception as e:
        print(f"Warning: Could not parse dedup log {f}: {e}")
    return stats

def parse_trim_json(f):
    sample = os.path.basename(f).split('_')[0]
    try:
        with open(f, 'r') as fh:
            data = json.load(fh)
            pct = data.get('filtering_statistics', {}).get('percent_trimmed', 0)
            if pct == 0:
                pct = data.get('report', {}).get('summary', {}).get('terminal_stats', {}).get('percent_trimmed', 0)
            return {'Sample': sample, 'Trimmed_Pct': pct}
    except: pass
    return {'Sample': sample, 'Trimmed_Pct': 0}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mapping_output")
    parser.add_argument("dedup_output")
    parser.add_argument("count_files", nargs="+")
    parser.add_argument("--dedup-logs", nargs="*")
    parser.add_argument("--trim-jsons", nargs="*")
    args = parser.parse_args()

    # 1. Pipeline Mapping Statistics
    dfs = []
    for f in args.count_files:
        sample = os.path.basename(f).replace('.tsv', '')
        try:
            df = pl.read_csv(f, separator='\t', has_header=False, new_columns=['Metric', sample])
            dfs.append(df)
        except: pass

    if dfs:
        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.join(df, on='Metric', how='outer_coalesced')
        
        desired_order = [
            "Raw", "Clean", 
            "Contamination_Passed", "Contamination_Dedup",
            "Masking_Passed", "Masking_Dedup",
            "Transcript_Passed", "Transcript_Dedup",
            "Genome_Passed", "Genome_Dedup"
        ]
        existing_metrics = [m for m in desired_order if m in df_final['Metric'].to_list()]
        others = [m for m in df_final['Metric'].to_list() if m not in desired_order]
        final_metrics = existing_metrics + others
        
        df_final = pl.DataFrame({"Metric": final_metrics}).join(df_final, on="Metric", how="left")
        df_mapping = df_final.drop('Metric').transpose(column_names=final_metrics)
        samples = [os.path.basename(f).replace('.tsv', '') for f in args.count_files]
        df_mapping = df_mapping.insert_column(0, pl.Series("Sample", samples))

        if args.trim_jsons:
            df_trim = pl.DataFrame([parse_trim_json(f) for f in args.trim_jsons])
            df_mapping = df_mapping.join(df_trim, on="Sample", how="left")

        header_mapping = [
            "# id: mapping_stats_table",
            "# section_name: 'Pipeline Mapping Statistics'",
            "# description: 'Read counts at each stage. Note: These are nested metrics (Clean < Raw, Passed < Clean, Dedup < Passed).'",
            "# plot_type: 'table'",
            "# pconfig:",
            "#    namespace: 'Mapping'",
            "#    format: '{:,.0f}'",
            "#    col_config:",
        ]
        for m in final_metrics:
            header_mapping.append(f"#        {m}: {{ 'plot_type': 'bar', 'min': 0 }}")
        header_mapping.append("#        Trimmed_Pct: {suffix: '%', scale: 'Purples', format: '{:.1f}'}")

        # Enhanced plot config
        header_mapping.extend([
            "# id: mapping_throughput_graph",
            "# section_name: 'Mapping Throughput Graph'",
            "# description: 'Read retention across pipeline stages. Grouped bars show the counts at each step.'",
            "# plot_type: 'bargraph'",
            "# pconfig:",
            "#    id: 'mapping_throughput_bargraph'",
            "#    title: 'Mapping Throughput'",
            "#    ylab: 'Number of Reads'",
            "#    stacking: false", # MultiQC uses false or null for side-by-side
        ])

        with open(args.mapping_output, 'w') as f_out:
            f_out.write("\n".join(header_mapping) + "\n")
            df_mapping.write_csv(f_out, separator='\t', include_header=True)

    # 2. Deduplication Statistics
    if args.dedup_logs:
        dedup_data = [parse_dedup_log(f) for f in args.dedup_logs]
        dedup_data = [d for d in dedup_data if 'Total_Reads' in d]
        if dedup_data:
            df_dedup = pl.DataFrame(dedup_data)
            df_dedup = df_dedup.with_columns(
                pl.format("{}_{}", pl.col("Sample"), pl.col("Type")).alias("Sample_Library")
            ).select(["Sample_Library", "Total_Reads", "Unique_Reads", "Duplicates", "Duplication_Rate"])
            header_dedup = [
                "# id: dedup_stats_table",
                "# section_name: 'Detailed Deduplication Statistics'",
                "# description: 'Deduplication metrics for all mapping targets.'",
                "# plot_type: 'table'",
                "# pconfig:",
                "#    namespace: 'Deduplication'",
                "#    format: '{:,.0f}'",
                "#    col_config:",
                "#        Duplication_Rate: {suffix: '%', scale: 'YlOrRd', format: '{:.2f}'}",
            ]
            with open(args.dedup_output, 'w') as f_out:
                f_out.write("\n".join(header_dedup) + "\n")
                df_dedup.write_csv(f_out, separator='\t', include_header=True)

if __name__ == "__main__":
    main()
