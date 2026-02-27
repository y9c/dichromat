#!/usr/bin/env python
import sys
import polars as pl
import os

def main():
    if len(sys.argv) < 3:
        print("Usage: aggregate_mapping_stats.py output.tsv input1.tsv input2.tsv ...")
        sys.exit(1)

    output_file = sys.argv[1]
    input_files = sys.argv[2:]

    dfs = []
    for f in input_files:
        sample = os.path.basename(f).replace('.tsv', '')
        try:
            # Read 2-column TSV: Metric, Value
            df = pl.read_csv(f, separator='\t', has_header=False, new_columns=['Metric', sample])
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")

    if not dfs:
        print("Error: No data aggregated.")
        sys.exit(1)

    # Join all samples on Metric
    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = df_final.join(df, on='Metric', how='outer_coalesced')
    
    # Transpose to get Sample as index
    # Metric becomes column names
    metrics = df_final['Metric'].to_list()
    df_transposed = df_final.drop('Metric').transpose(column_names=metrics)
    
    # Add Sample column back
    # The transpose result column names are the metrics, the "index" is now the sample names from the filenames
    # But Polars transpose doesn't keep the original column names as a column by default in all versions
    # Let's be explicit.
    samples = [os.path.basename(f).replace('.tsv', '') for f in input_files]
    df_transposed = df_transposed.insert_column(0, pl.Series("Sample", samples))

    # MultiQC configuration header
    header = [
        "# id: mapping_stats_table",
        "# section_name: 'Pipeline Mapping Statistics'",
        "# description: 'Aggregated read counts at various stages of the pipeline (Raw, Clean, Contamination, Masking, Transcript, Genome).'",
        "# plot_type: 'table'",
        "# pconfig:",
        "#    namespace: 'eTAM-seq Mapping'",
    ]
    
    with open(output_file, 'w') as f_out:
        f_out.write("\n".join(header) + "\n")
        # Use include_header=True for the actual data
        df_transposed.write_csv(f_out, separator='\t', include_header=True)

if __name__ == "__main__":
    main()
