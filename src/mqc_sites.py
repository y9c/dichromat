#!/usr/bin/env python
"""polars-free version of mqc_sites.py; heavy aggregation pushed into duckdb
(columnar pushdown + spilling => lower RAM than full parquet loads).

Produces the same QC tables as the polars version:
  * motif conversion tables (transcript / genome)
  * per-library summary (count, mean depth, mean/max ratio)
  * per-library ratio & depth histograms (identical bins)

Division-by-zero (u/d, d=0) follows IEEE like polars (inf/nan).
"""

import argparse
import logging
import os
import sys

import duckdb
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _fmt(v):
    """Match polars' TSV float/int rendering closely ("NaN"/"inf" casing)."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        if v != v:          # NaN
            return "NaN"
        return str(v)       # inf / -inf / finite (py3 str(v)==repr(v))
    return str(v)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("heatmap_output")
    parser.add_argument("summary_output")
    parser.add_argument("dist_output")
    parser.add_argument("depth_output")
    parser.add_argument("transcript_table_output")
    parser.add_argument("genome_table_output")
    parser.add_argument("--motif-files", nargs="+")
    parser.add_argument("--sites-file", nargs="+")
    parser.add_argument("--target-base", default="A")
    args = parser.parse_args()

    if not args.sites_file:
        logging.error("No sites file provided.")
        return

    try:
        con = duckdb.connect()
        parts = []
        for f in args.sites_file:
            if f.endswith(".parquet"):
                parts.append(f"(SELECT * FROM read_parquet('{f}'))")
            else:
                parts.append(f"(SELECT * FROM read_csv_auto('{f}', delim='\\t', header=true))")
        sites_sql = "\nUNION ALL BY NAME\n".join(parts)

        depth_cols = sorted([c for c in con.sql(f"DESCRIBE SELECT * FROM ({sites_sql})")
                             .fetchall() if c[0].startswith("Depth_")])
        depth_cols = [c[0] for c in depth_cols]
        libraries = [c.replace("Depth_", "") for c in depth_cols]
        if not libraries:
            logging.warning("No depth columns found in sites file.")
            return

        # 1. MOTIF HEATMAP tables (final)
        if args.motif_files:
            logging.info("Processing motif conversion files...")
            motif_parts = []
            for f in args.motif_files:
                sample_name = os.path.basename(f).split(".")[0]
                reftype = "transcript" if "transcript" in f else "genome"
                motif_parts.append(
                    f"(SELECT *, '{sample_name}' AS sample, '{reftype}' AS reftype"
                    f" FROM read_csv_auto('{f}', delim='\\t', header=true))")
            if motif_parts:
                agg_sql = (
                    "SELECT Motif, reftype,"
                    " SUM(CAST(Unconverted AS DOUBLE))/SUM(CAST(Depth AS DOUBLE)) AS Ratio"
                    f" FROM ({' UNION ALL BY NAME '.join(motif_parts)})"
                    " GROUP BY Motif, reftype")
                rows = con.sql(agg_sql).fetchall()
                # Write the full aggregated motif heatmap (Motif, reftype, Ratio)
                with open(args.heatmap_output, "w", newline="") as fh:
                    fh.write("\t".join(["Motif", "reftype", "Ratio"]) + "\n")
                    for m, t, r in sorted(rows, key=lambda x: (x[1], x[0])):
                        fh.write("\t".join([str(m), str(t), _fmt(r)]) + "\n")
                for reftype, out_path in [("transcript", args.transcript_table_output),
                                          ("genome", args.genome_table_output)]:
                    sub = sorted([(m, r) for m, t, r in rows if t == reftype],
                                 key=lambda x: x[0])
                    if not sub:
                        continue
                    with open(out_path, "w", newline="") as fh:
                        fh.write("\t".join(str(m) for m, _ in sub) + "\n")
                        fh.write("\t".join(_fmt(r) for _, r in sub) + "\n")

        # 2. Per-library summary (columnar aggregate, tiny result => low RAM)
        logging.info("Aggregating site summaries in duckdb...")
        aggs = []
        for lib in libraries:
            dcol = f"Depth_{lib}"
            ucol = f"Uncon_{lib}"
            aggs.append(f'COUNT("{dcol}") AS "cnt_{lib}"')
            aggs.append(f'SUM(CAST("{dcol}" AS DOUBLE)) AS "sumd_{lib}"')
            aggs.append(f'SUM(CAST("{ucol}" AS DOUBLE)/CAST("{dcol}" AS DOUBLE)) AS "sumr_{lib}"')
            aggs.append(f'MAX(CAST("{ucol}" AS DOUBLE)/CAST("{dcol}" AS DOUBLE)) AS "maxr_{lib}"')
        one = con.sql(f"SELECT {', '.join(aggs)} FROM ({sites_sql})").fetchone()

        summary_rows = []
        for i, lib in enumerate(libraries):
            cnt = one[4 * i]
            if cnt is not None and cnt > 0:
                summary_rows.append({
                    "Sample": lib,
                    "Total Sites": int(cnt),
                    "Mean Depth": one[4 * i + 1] / cnt,
                    "Mean Ratio": one[4 * i + 2] / cnt,
                    "Max Ratio": one[4 * i + 3],
                })
        if summary_rows:
            cols = ["Sample", "Total Sites", "Mean Depth", "Mean Ratio", "Max Ratio"]
            with open(args.summary_output, "w", newline="") as fh:
                fh.write("\t".join(cols) + "\n")
                for r in summary_rows:
                    fh.write("\t".join(_fmt(v) for v in
                             [r["Sample"], r["Total Sites"], r["Mean Depth"],
                              r["Mean Ratio"], r["Max Ratio"]]) + "\n")

        # 3. Histograms (per library; one column at a time)
        logging.info("Building histograms...")
        ratio_bins = np.linspace(0, 1, 51)
        depth_bins = np.logspace(0, 5, 51)
        ratio_keys = ["Ratio"] + libraries
        depth_keys = ["Depth"] + libraries
        ratio_hist = [[f"{(a + b) / 2:.2f}" for a, b in zip(ratio_bins[:-1], ratio_bins[1:])]]
        depth_hist = [[int((a + b) // 2) for a, b in zip(depth_bins[:-1], depth_bins[1:])]]
        for lib in libraries:
            dcol, ucol = f"Depth_{lib}", f"Uncon_{lib}"
            d_vals = np.asarray(
                con.sql(f'SELECT "{dcol}" FROM ({sites_sql}) WHERE "{dcol}" > 0')
                .fetchall(), dtype=np.float64).ravel()
            u_vals = np.asarray(
                con.sql(f'SELECT "{ucol}" FROM ({sites_sql}) WHERE "{dcol}" > 0')
                .fetchall(), dtype=np.float64).ravel()
            if len(d_vals):
                r_c, _ = np.histogram(u_vals / d_vals, bins=ratio_bins)
                d_c, _ = np.histogram(d_vals, bins=depth_bins)
                ratio_hist.append([int(x) for x in r_c])
                depth_hist.append([int(x) for x in d_c])

        def _write_hist(path, keys, hist):
            with open(path, "w", newline="") as fh:
                fh.write("\t".join(keys) + "\n")
                for i in range(len(hist[0])):
                    fh.write("\t".join(_fmt(col[i]) for col in hist) + "\n")

        _write_hist(args.dist_output, ratio_keys, ratio_hist)
        _write_hist(args.depth_output, depth_keys, depth_hist)
        logging.info("MQC Site Aggregation Complete.")

    except Exception as e:
        logging.error(f"Error in mqc_sites: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
