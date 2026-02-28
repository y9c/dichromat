#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2025 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Created: 2025-01-21 21:21

import argparse
import logging
import pickle
from collections import defaultdict

import numpy as np
import polars as pl
from scipy.optimize import curve_fit
from scipy.stats import chi2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

MIN_DEPTH = 10
MIN_RATIO = 0.1


def expected_unconverted_rate(x, a, b):
    return a * np.exp(b * x)


def expected_m6A_level(x, c, d, e):
    return c * np.exp(-d * (x - e) ** 2)


def combined_function(x, a, b, c, d, e):
    m = expected_m6A_level(x, c, d, e)
    return m + (1 - m) * expected_unconverted_rate(x, a, b)


def fit_motif(x_data, y_data):
    # Check minimum data requirements
    if len(x_data) < 5:
        raise ValueError(f"Insufficient data points: {len(x_data)} (minimum 5)")
    
    # Initial guesses for a, b in expected_m6A_level
    initial_guesses_m6A = [0.03, 10, 0.4]
    # Bounds for a, b to improve fitting
    bounds_m6A = ([0, 1, 0.25], [0.3, 100, 0.8])

    # Updated initial guesses for a, b in expected_unconverted_rate
    initial_guesses_unconverted = [0.0001, 8]
    # Updated bounds for a, b to better constrain the fit
    bounds_unconverted = ([0, 2], [0.0005, 25])

    # Fit the m6A level using all data points
    params_m6A, _ = curve_fit(
        expected_m6A_level,
        x_data.filter(x_data < 0.5),
        y_data.filter(x_data < 0.5),
        p0=initial_guesses_m6A,
        bounds=bounds_m6A,
        maxfev=10_000,
    )

    # Calculate residuals for unconverted rate fitting
    m6A_level = expected_m6A_level(x_data, *params_m6A)
    y_residual = y_data - m6A_level

    # Fit the unconverted rate using the residuals
    params_unconverted, _ = curve_fit(
        expected_unconverted_rate,
        x_data,
        y_residual,
        p0=initial_guesses_unconverted,
        bounds=bounds_unconverted,
        maxfev=10_000,
    )

    # Use initial fits as prior to fit the combined function again
    combined_initial_guess = [*params_unconverted, *params_m6A]
    bounds_combined = (
        bounds_unconverted[0] + bounds_m6A[0],
        bounds_unconverted[1] + bounds_m6A[1],
    )
    params_combined, _ = curve_fit(
        combined_function,
        x_data,
        y_data,
        p0=combined_initial_guess,
        bounds=bounds_combined,
        maxfev=20_000,
    )

    return params_combined


def calculate_background_fitting(df_sites, libraries):
    library2background = defaultdict(dict)
    library2gcfit = defaultdict(dict)

    min_GC = 0.2
    max_GC = 0.8
    for library in libraries:
        logging.info(f"  Calculating background and fitting for {library}")
        df = (
            df_sites.filter(
                ~(pl.col("Motif3").str.contains("N"))
                & (pl.col(f"Depth_{library}") >= 1)
                & (pl.col(f"Depth_{library}") < 100_000)
            )
            .with_columns(
                Ratio=(pl.col(f"Uncon_{library}") / pl.col(f"Depth_{library}")).round(2)
            )
            .group_by(["Motif3", "GC", "Ratio"])
            .agg(Count=pl.col("Pos").len())
            .with_columns(
                GC_bin=pl.when(pl.col("GC") > max_GC)
                .then(1)
                .when(pl.col("GC") < min_GC)
                .then(0)
                .otherwise(pl.col("GC"))
            )
            .group_by("Motif3", "GC_bin")
            .agg(
                Ratio=pl.col("Ratio").repeat_by(pl.col("Count")).list.explode().mean(),
                GC=pl.col("GC").repeat_by(pl.col("Count")).list.explode().mean(),
                Count=pl.col("Count").sum(),
            )
            .sort("Motif3", "GC")
        )

        for (m,), df2 in df.group_by("Motif3", maintain_order=True):
            df2 = df2.sort("GC").select("GC", "Ratio", "Count")
            x, y, z = df2["GC"], df2["Ratio"], df2["Count"]
            library2background[library][m] = (x, y, z)
            
            # Data quality checks before fitting
            x_list = x.to_list()
            y_list = y.to_list()
            skip_reason = None
            
            if len(x_list) < 5:
                skip_reason = f"insufficient data ({len(x_list)} < 5)"
            else:
                y_array = np.array(y_list)
                y_min, y_max = np.min(y_array), np.max(y_array)
                y_range = y_max - y_min
                y_mean = np.mean(y_array)
                
                if y_range < 1e-10:
                    skip_reason = f"no variance in data"
                elif y_mean > 0 and y_range / y_mean < 0.05:
                    skip_reason = f"insufficient variation"
                elif len(set(x_list)) < 3:
                    skip_reason = f"insufficient unique GC values"
            
            if skip_reason:
                logging.info(f"    Skipping fit for motif {m}: {skip_reason}")
                library2gcfit[library][m] = [0.0001, 8, 0.03, 10, 0.4]
                continue
            
            try:
                library2gcfit[library][m] = fit_motif(
                    pl.Series(x_list), pl.Series(y_list)
                )
            except Exception as e:
                logging.warning(f"    Failed to fit motif {m} for {library}: {e}")
                library2gcfit[library][m] = [0.0001, 8, 0.03, 10, 0.4]  # Fallback

    return library2background, library2gcfit


def validate_site_vectorized(u, d, b):
    # u, d, b are NumPy arrays
    r = np.zeros_like(d, dtype=float)
    valid = d > 0
    r[valid] = u[valid] / d[valid]
    
    b = np.maximum(0.0001, b)
    
    # Calculate Chi-Square statistic
    expected_u = d * b
    expected_c = d * (1 - b)
    c = d - u
    
    # Avoid division by zero in expected calculations
    expected_u = np.where(expected_u <= 0, 1e-10, expected_u)
    expected_c = np.where(expected_c <= 0, 1e-10, expected_c)
    
    chi2_stat = ((u - expected_u)**2 / expected_u) + ((c - expected_c)**2 / expected_c)
    
    # Calculate p-value
    p_vals = chi2.sf(chi2_stat, df=1)
    
    # Apply conditions (where d <= MIN_DEPTH, r < MIN_RATIO, etc., p_val = 1.0)
    mask = (d <= MIN_DEPTH) | (d == 0) | (r < MIN_RATIO) | (r <= b) | (r == 0)
    p_vals[mask] = 1.0
    return p_vals


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=True, help="Input file")
    ap.add_argument("-o", "--output", required=True, help="Output file")

    args = ap.parse_args()
    input_file = args.input
    output_file = args.output

    logging.info("Reading input file")
    
    # First, peek at the file to get column names for schema overrides
    df_test = pl.read_csv(input_file, separator="\t", has_header=True, n_rows=5)
    
    # Build schema overrides - ensure Depth_* and Uncon_* columns are numeric
    schema_overrides = {"Chrom": pl.Utf8, "GenePos": pl.Utf8, "GeneName": pl.Utf8}
    for col in df_test.columns:
        if col.startswith("Depth_") or col.startswith("Uncon_"):
            schema_overrides[col] = pl.Int64
    
    # Get list of numeric columns to cast
    numeric_cols = [col for col in df_test.columns 
                   if col.startswith("Depth_") or col.startswith("Uncon_")]
    
    df_sites = (
        pl.scan_csv(
            input_file,
            separator="\t",
            has_header=True,
            schema_overrides=schema_overrides,
        )
        .with_columns(
            Motif3=pl.col("Motif")
            .str.slice(14, 3)
            .str.to_uppercase()
            .str.replace("T", "U", n=2),
            GC=pl.col("Motif")
            .map_elements(
                lambda x: (x[14 - 10 : 14] + x[17 : 17 + 10])
                .upper()
                .replace("G", "C")
                .count("C")
                / 20,
                return_dtype=pl.Float64,
            )
            .round(2),
        )
        .filter((pl.col("Strand") != ".") & ~(pl.col("Motif3").str.contains("N")))
        .collect()
    )
    
    # Explicitly cast numeric columns to ensure correct types (handles empty files)
    if numeric_cols:
        df_sites = df_sites.with_columns([
            pl.col(col).cast(pl.Int64) for col in numeric_cols if col in df_sites.columns
        ])

    _samples_with_fto = list(
        set(
            [
                c.removeprefix("Depth_").split("-p-")[0]
                for c in df_sites.columns
                if "-p-" in c and c.startswith("Depth_")
            ]
        )
    )
    if _samples_with_fto:
        df_sites = df_sites.with_columns(
            [
                pl.sum_horizontal(pl.col(f"^Depth_{s}-p-.*$")).alias(f"Depth_{s}-FTO")
                for s in _samples_with_fto
            ]
            + [
                pl.sum_horizontal(pl.col(f"^Uncon_{s}-p-.*$")).alias(f"Uncon_{s}-FTO")
                for s in _samples_with_fto
            ]
        ).drop(pl.selectors.contains("-p-") & pl.selectors.starts_with("Depth_", "Uncon_"))

    libraries = [c.removeprefix("Depth_") for c in df_sites.columns if "Depth_" in c]
    
    if not libraries:
        logging.warning("No Depth_* columns found in input file. Cannot process sites.")
        # Write empty output with header
        pl.DataFrame().write_csv(output_file, separator="\t")
        exit(0)
    
    if len(df_sites) == 0:
        logging.warning("Input file has no data rows. Writing empty output.")
        pl.DataFrame().write_csv(output_file, separator="\t")
        exit(0)

    logging.info(f"Processing {len(df_sites)} sites for libraries: {libraries}")
    logging.info("Calculating background and fitting")
    library2background, library2gcfit = calculate_background_fitting(df_sites, libraries)
    
    pickle.dump(
        library2background, open(args.output.removesuffix(".tsv") + ".background.pkl", "wb")
    )
    pickle.dump(library2gcfit, open(args.output.removesuffix(".tsv") + ".params.pkl", "wb"))
    
    df_ft = pl.DataFrame(
        [
            (f"Background_{k}", m, round(i, 2), expected_unconverted_rate(i, *v2[:2]))
            for i in np.arange(0, 1.01, 0.01) # More fine-grained GC
            for k, v in library2gcfit.items()
            for m, v2 in v.items()
        ],
        orient="row",
        schema={
            "library": pl.String,
            "Motif3": pl.String,
            "GC": pl.Float32,
            "bg": pl.Float64,
        },
    ).pivot(index=["Motif3", "GC"], on="library", values="bg")

    logging.info("Combining background statistics")
    df_sites = df_sites.select(pl.all().exclude("^Background_.*$")).join(
        df_ft, on=["Motif3", "GC"], how="left"
    )

    logging.info("Validating sites")
    for library in libraries:
        logging.info(f"  Validating sites for {library}")
        df_sites = df_sites.with_columns(
            (pl.col(f"Uncon_{library}") / pl.col(f"Depth_{library}")).alias(
                f"Ratio_{library}"
            ),
            pl.struct(f"Uncon_{library}", f"Depth_{library}", f"Background_{library}")
            .map_batches(
                lambda s, library=library: pl.Series(validate_site_vectorized(
                    s.struct.field(f"Uncon_{library}").to_numpy(),
                    s.struct.field(f"Depth_{library}").to_numpy(),
                    s.struct.field(f"Background_{library}").to_numpy(),
                )),
                return_dtype=pl.Float64,
            )
            .alias(f"p_{library}"),
        )

    logging.info("Filtering and writing output file")
    df_filtered = df_sites.filter(pl.any_horizontal(pl.col("^p_.*$") < 1))
    df_filtered.write_csv(
        output_file,
        separator="\t",
        float_precision=8,
        float_scientific=True,
        batch_size=4096,
    )
