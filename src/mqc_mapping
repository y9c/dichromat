#!/usr/bin/env python
"""polars-free version of mqc_mapping.py (results identical).

Builds the MultiQC-style mapping/dedup TSV tables for report_html from the
pipeline's count/dedup/trim files. polars was only used for small-table
sort/join/CSV; stdlib csv is used instead (same columns, order, formatting).
"""

import argparse
import csv
import json
import os
import re
import sys


def parse_dedup_log(f):
    basename = os.path.basename(f)
    parts = basename.split(".")
    sample = parts[0]
    reftype = parts[1]
    stats = {"Sample": sample, "Type": reftype}
    try:
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                m = re.search(r"processed:\s*([0-9,]+)", line)
                if m:
                    stats["Total_Reads"] = int(m.group(1).replace(",", ""))
                    continue
                m = re.search(r"removed:\s*([0-9,]+)", line)
                if m:
                    stats["Duplicates"] = int(m.group(1).replace(",", ""))
                    continue
                m = re.search(r"Unique Reads:\s*([0-9,]+)", line)
                if m:
                    stats["Unique_Reads"] = int(m.group(1).replace(",", ""))
                    continue
                m = re.search(r"rate:\s*([\d\.]+)%", line)
                if m:
                    stats["Duplication_Rate"] = float(m.group(1))
    except Exception as e:
        print(f"Warning: Could not parse dedup log {f}: {e}")
    return stats


def parse_trim_json(f):
    sample = os.path.basename(f).split("_")[0]
    try:
        with open(f, "r") as fh:
            data = json.load(fh)
            pct = data.get("filtering_statistics", {}).get("percent_trimmed", 0)
            if pct == 0:
                pct = (data.get("report", {}).get("summary", {})
                       .get("terminal_stats", {}).get("percent_trimmed", 0))
            return {"Sample": sample, "Trimmed_Pct": pct}
    except Exception:
        pass
    return {"Sample": sample, "Trimmed_Pct": 0}


def _write_table(path, header_lines, columns, rows):
    with open(path, "w", newline="") as f:
        f.write("\n".join(header_lines) + "\n")
        w = csv.DictWriter(f, fieldnames=columns, delimiter="\t", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r)


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
        sample = os.path.basename(f).replace(".tsv", "")
        try:
            counts = {}
            with open(f, "r") as fh:
                for line in fh:
                    k, v = line.strip().split("\t")
                    counts[k] = int(v)

            raw = counts.get("Raw", 0)
            clean = counts.get("Clean", 0)
            contam = counts.get("Contamination_Passed", 0)
            masking = counts.get("Masking_Passed", 0)
            target_passed = counts.get("Transcript_Passed", 0) + counts.get("Genome_Passed", 0)
            target_unique = counts.get("Transcript_Dedup", 0) + counts.get("Genome_Dedup", 0)

            unique_m = target_unique
            duplicates_m = max(0, target_passed - target_unique)
            masking_m = masking
            contam_m = contam
            unmapped_m = max(0, clean - (contam + masking + target_passed))
            discarded_m = max(0, raw - clean)

            results.append({
                "Sample": sample,
                "Unique Mapped (Target)": unique_m,
                "Duplicates": duplicates_m,
                "Masking": masking_m,
                "Contamination": contam_m,
                "Unmapped": unmapped_m,
                "Discarded (Short)": discarded_m,
            })
        except Exception as e:
            print(f"Error processing {f}: {e}")

    if results:
        results.sort(key=lambda r: r["Sample"])
        columns = list(results[0].keys())
        if args.trim_jsons:
            trim = {d["Sample"]: d["Trimmed_Pct"] for d in
                    [parse_trim_json(f) for f in args.trim_jsons]}
            cols_new = ["Sample", "Unique Mapped (Target)", "Duplicates", "Masking",
                        "Contamination", "Unmapped", "Discarded (Short)", "Trimmed_Pct"]
            for r in results:
                r["Trimmed_Pct"] = trim.get(r["Sample"], None)
            columns = cols_new

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
        _write_table(args.mapping_output, header_mapping, columns, results)

    # 2. Deduplication Statistics
    if args.dedup_logs:
        dedup_data = [d for d in (parse_dedup_log(f) for f in args.dedup_logs)
                      if "Total_Reads" in d]
        if dedup_data:
            columns = ["Sample_Library", "Total_Reads", "Unique_Reads",
                       "Duplicates", "Duplication_Rate"]
            rows = []
            for d in dedup_data:
                rows.append({
                    "Sample_Library": f"{d['Sample']}_{d['Type']}",
                    "Total_Reads": d["Total_Reads"],
                    "Unique_Reads": d["Unique_Reads"],
                    "Duplicates": d["Duplicates"],
                    "Duplication_Rate": d["Duplication_Rate"],
                })
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
            _write_table(args.dedup_output, header_dedup, columns, rows)


if __name__ == "__main__":
    main()
