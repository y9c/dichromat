#!/usr/bin/env python
import sys
import polars as pl
import os
import argparse
import re
import json

def parse_dedup_log(f):
    """Parse markdup log file for key metrics"""
    stats = {}
    sample = os.path.basename(f).split('.')[0]
    stats['Sample'] = sample
    try:
        with open(f, 'r') as fh:
            content = fh.read()
            m_total = re.search(r"Total reads processed: (\d+)", content)
            m_dup = re.search(r"Duplicates removed: (\d+)", content)
            m_unique = re.search(r"Total Unique Reads: (\d+)", content)
            m_rate = re.search(r"Deduplication rate: ([\d\.]+)%", content)
            if m_total: stats['Total_Reads'] = int(m_total.group(1))
            if m_dup: stats['Duplicates'] = int(m_dup.group(1))
            if m_unique: stats['Unique_Reads'] = int(m_unique.group(1))
            if m_rate: stats['Duplication_Rate'] = float(m_rate.group(1))
    except Exception as e:
        print(f"Warning: Could not parse dedup log {f}: {e}")
    return stats

def parse_trim_json(f):
    """Parse cutseq/cutadapt JSON for trimming percentage"""
    sample = os.path.basename(f).split('_')[0]
    try:
        with open(f, 'r') as fh:
            data = json.load(fh)
            # Extract from 'report' -> 'summary' -> 'terminal_stats' or similar
            # In cutseq/cutadapt JSON, it is usually under 'filtering_statistics'
            pct = data.get('filtering_statistics', {}).get('percent_trimmed', 0)
            return {'Sample': sample, 'Trimmed_Pct': pct}
    except Exception as e:
        print(f"Warning: Could not parse trim JSON {f}: {e}")
    return {'Sample': sample, 'Trimmed_Pct': 0}

def main():
    parser = argparse.ArgumentParser(description="Aggregate mapping and dedup stats for MultiQC")
    parser.add_argument("mapping_output", help="Output mapping TSV")
    parser.add_argument("dedup_output", help="Output dedup TSV")
    parser.add_argument("count_files", nargs="+", help="Input count TSV files")
    parser.add_argument("--dedup-logs", nargs="*", help="Input markdup log files")
    parser.add_argument("--trim-jsons", nargs="*", help="Input trimming JSON files")
    
    args = parser.parse_args()

    # 1. Process Mapping Counts
    dfs = []
    for f in args.count_files:
        sample = os.path.basename(f).replace('.tsv', '')
        try:
            df = pl.read_csv(f, separator='\t', has_header=False, new_columns=['Metric', sample])
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")

    if dfs:
        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.join(df, on='Metric', how='outer_coalesced')
        
        metrics = df_final['Metric'].to_list()
        df_mapping = df_final.drop('Metric').transpose(column_names=metrics)
        samples = [os.path.basename(f).replace('.tsv', '') for f in args.count_files]
        df_mapping = df_mapping.insert_column(0, pl.Series("Sample", samples))

        # Add Trimming data if provided
        if args.trim_jsons:
            trim_data = [parse_trim_json(f) for f in args.trim_jsons]
            df_trim = pl.DataFrame(trim_data)
            df_mapping = df_mapping.join(df_trim, on="Sample", how="left")

        header_mapping = [
            "# id: mapping_stats_table",
            "# section_name: 'Pipeline Mapping Statistics'",
            "# description: 'Aggregated read counts across pipeline stages.'",
            "# plot_type: 'table'",
            "# pconfig:",
            "#    namespace: 'Mapping'",
            "#    format: '{:,.0f}'",
            "#    col_config:",
            "#        Trimmed_Pct:",
            "#            suffix: '%'",
            "#            scale: 'Purples'",
            "#            format: '{:.1f}'",
        ]
        with open(args.mapping_output, 'w') as f_out:
            f_out.write("\n".join(header_mapping) + "\n")
            df_mapping.write_csv(f_out, separator='\t', include_header=True)

    # 2. Process Dedup Logs
    if args.dedup_logs:
        dedup_data = [parse_dedup_log(f) for f in args.dedup_logs]
        if dedup_data:
            df_dedup = pl.DataFrame(dedup_data)
            header_dedup = [
                "# id: dedup_stats_table",
                "# section_name: 'Deduplication Statistics (Genome)'",
                "# description: 'Statistics from markdup deduplication on genome-aligned reads.'",
                "# plot_type: 'table'",
                "# pconfig:",
                "#    namespace: 'Deduplication'",
                "#    col_config:",
                "#        Duplication_Rate:",
                "#            suffix: '%'",
                "#            scale: 'YlOrRd'",
                "#            format: '{:.2f}'",
            ]
            with open(args.dedup_output, 'w') as f_out:
                f_out.write("\n".join(header_dedup) + "\n")
                df_dedup.write_csv(f_out, separator='\t', include_header=True)

if __name__ == "__main__":
    main()
