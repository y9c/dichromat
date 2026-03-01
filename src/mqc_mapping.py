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
            content = fh.read()
            # Use [0-9,]+ to handle thousands separators, then strip commas
            m_total = re.search(r"Total reads processed: ([0-9,]+)", content)
            m_dup = re.search(r"Duplicates removed: ([0-9,]+)", content)
            m_unique = re.search(r"Total Unique Reads: ([0-9,]+)", content)
            m_rate = re.search(r"Deduplication rate: ([\d\.]+)%", content)
            
            if m_total: stats['Total_Reads'] = int(m_total.group(1).replace(',', ''))
            if m_dup: stats['Duplicates'] = int(m_dup.group(1).replace(',', ''))
            if m_unique: stats['Unique_Reads'] = int(m_unique.group(1).replace(',', ''))
            if m_rate: stats['Duplication_Rate'] = float(m_rate.group(1))
    except Exception as e:
        print(f"Warning: Could not parse dedup log {f}: {e}")
    return stats

def parse_trim_json(f):
    sample = os.path.basename(f).split('_')[0]
    try:
        with open(f, 'r') as fh:
            data = json.load(fh)
            pct = data.get('filtering_statistics', {}).get('percent_trimmed', 0)
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

    # 1. Pipeline Mapping Statistics (Table + Plot)
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
        
        metrics = df_final['Metric'].to_list()
        df_mapping = df_final.drop('Metric').transpose(column_names=metrics)
        samples = [os.path.basename(f).replace('.tsv', '') for f in args.count_files]
        df_mapping = df_mapping.insert_column(0, pl.Series("Sample", samples))

        if args.trim_jsons:
            df_trim = pl.DataFrame([parse_trim_json(f) for f in args.trim_jsons])
            df_mapping = df_mapping.join(df_trim, on="Sample", how="left")

        # Config for Mapping Throughput Plot
        header_mapping = [
            "# id: mapping_stats_table",
            "# section_name: 'Pipeline Mapping Statistics'",
            "# description: 'Read counts at each stage. Note: These are nested metrics (Clean < Raw, Passed < Clean, Dedup < Passed).'",
            "# plot_type: 'table'",
            "# pconfig:",
            "#    namespace: 'Mapping'",
            "#    format: '{:,.0f}'",
            "#    col_config:",
            "#        Trimmed_Pct: {suffix: '%', scale: 'Purples', format: '{:.1f}'}",
            "# id: mapping_stats_plot",
            "# section_name: 'Mapping Throughput Graph'",
            "# description: 'Visual comparison of read counts at different stages. Columns are grouped (not stacked) to show the reduction at each step.'",
            "# plot_type: 'bargraph'",
            "# pconfig:",
            "#    id: 'mapping_stats_bargraph'",
            "#    title: 'Mapping Throughput'",
            "#    ylab: 'Number of Reads'",
            "#    stacking: false", # Key fix: prevent adding them up
        ]

        with open(args.mapping_output, 'w') as f_out:
            f_out.write("\n".join(header_mapping) + "\n")
            df_mapping.write_csv(f_out, separator='\t', include_header=True)

    # 2. Deduplication Statistics
    if args.dedup_logs:
        dedup_data = [parse_dedup_log(f) for f in args.dedup_logs]
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
