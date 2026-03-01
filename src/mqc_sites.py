#!/usr/bin/env python
import sys
import polars as pl
import os

def main():
    if len(sys.argv) < 3:
        print("Usage: mqc_sites.py output.tsv input1.tsv input2.tsv ...")
        sys.exit(1)

    output_file = sys.argv[1]
    input_files = sys.argv[2:]

    all_dfs = []
    for f in input_files:
        filename = os.path.basename(f)
        sample = filename.split('.')[0]
        try:
            df = pl.read_csv(f, separator='\t')
            df = df.with_columns(
                pl.col('Motif').str.to_uppercase().str.replace_all('T', 'U'),
                pl.lit(sample).alias('Sample')
            )
            all_dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")

    if not all_dfs:
        print("Error: No data aggregated.")
        sys.exit(1)

    df_concat = pl.concat(all_dfs)
    df_pivot = df_concat.pivot(on='Sample', index='Motif', values='Ratio')
    df_pivot = df_pivot.sort("Motif")

    header = [
        "# id: motif_conversion_heatmap",
        "# section_name: 'Motif Conversion Ratios'",
        "# description: 'Heatmap showing the global conversion ratio (Unconverted / Depth) for all 3-mer motifs.'",
        "# plot_type: 'heatmap'",
        "# pconfig:",
        "#    title: 'Motif Conversion Ratios'",
        "#    x_title: 'Sample'",
        "#    y_title: '3-mer Motif'",
        "#    min: 0",
        "#    max: 1",
        "#    stops: [[0, '#f7fcf0'], [0.5, '#7bccc4'], [1, '#084081']]",
    ]

    with open(output_file, 'w') as f_out:
        f_out.write("\n".join(header) + "\n")
        df_pivot.write_csv(f_out, separator='\t', include_header=True)

if __name__ == "__main__":
    main()
