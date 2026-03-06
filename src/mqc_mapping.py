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
    results = []
    for f in args.count_files:
        sample = os.path.basename(f).replace('.tsv', '')
        try:
            # Read counts into a dict
            counts = {}
            with open(f, 'r') as fh:
                for line in fh:
                    k, v = line.strip().split('\t')
                    counts[k] = int(v)
            
            raw = counts.get('Raw', 0)
            clean = counts.get('Clean', 0)
            contam = counts.get('Contamination_Passed', 0)
            masking = counts.get('Masking_Passed', 0)
            target_passed = counts.get('Transcript_Passed', 0) + counts.get('Genome_Passed', 0)
            target_unique = counts.get('Transcript_Dedup', 0) + counts.get('Genome_Dedup', 0)

            # Calculation of Delta Segments (Hierarchy)
            unique_m = target_unique
            duplicates_m = max(0, target_passed - target_unique)
            masking_m = masking
            contam_m = contam
            unmapped_m = max(0, clean - (contam + masking + target_passed))
            discarded_m = max(0, raw - clean)

            res = {
                "Sample": sample,
                "Unique Mapped (Target)": unique_m,
                "Duplicates": duplicates_m,
                "Masking": masking_m,
                "Contamination": contam_m,
                "Unmapped": unmapped_m,
                "Discarded (Short)": discarded_m,
                "Total_Raw": raw
            }
            results.append(res)
        except Exception as e:
            print(f"Error processing {f}: {e}")

    if results:
        df_mapping = pl.DataFrame(results).sort("Sample")
        
        if args.trim_jsons:
            df_trim = pl.DataFrame([parse_trim_json(f) for f in args.trim_jsons])
            df_mapping = df_mapping.join(df_trim, on="Sample", how="left")

        # MultiQC Header with specific colors from the user snippet
        header_mapping = [
            "# id: mapping_stats_table",
            "# section_name: 'Pipeline Mapping Statistics'",
            "# description: 'Read counts at each stage. Bars are stacked to show the full hierarchy of read retention.'",
            "# plot_type: 'bargraph'",
            "# pconfig:",
            "#    id: 'mapping_hierarchy_bargraph'",
            "#    title: 'Sequencing Read Alignment Hierarchy'",
            "#    ylab: 'Number of Reads'",
            "#    stacking: 'normal'",
            "#    cpswitch: false",
            "#    colors:",
            "#        'Unique Mapped (Target)': '#1b5e20'",
            "#        'Duplicates': '#4caf50'",
            "#        'Masking': '#7b1fa2'",
            "#        'Contamination': '#9c27b0'",
            "#        'Unmapped': '#81d4fa'",
            "#        'Discarded (Short)': '#ff8a65'",
        ]

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
