#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2025 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Per-motif enrichment & conversion summary for the final HTML report.
#
# Combines two existing pipeline products:
#   * stats/ratio/by_motif/{sample}.genome.tsv
#       per-3-mer genome-wide pileup stats from the 8-column countmut view
#       (chrom pos strand motif u0 u1 m0 m1): candidates + depth-weighted
#       A->G conversion over the gated (group 1) and all kept reads.
#   * report_sites/filtered.tsv
#       eTAM sites passing filter_sites, with a p_<sample> column per library.
#
# Writes one row per trinucleotide (RNA, T->U) with filtered sites > 0:
#   Motif  Candidates  Filtered  Enrichment  ConvHQ  ConvAll
#   Candidates = Count_all           (genomic candidates with >= 1 kept base)
#   Filtered   = # filtered rows with p_<sample> < 1
#   Enrichment = Filtered/Candidates*1000  (sites per 1,000 candidates)
#   ConvHQ     = 100*(Depth-Unconverted)/Depth              (m1/(u1+m1))
#   ConvAll    = 100*(Depth_all-Unconverted_all)/Depth_all  (all kept groups)
#
# Usage:
#   motif_enrich.py -i BY_MOTIF.tsv -f FILTERED.tsv -s SAMPLE -o OUT.tsv [-b A]

import argparse
import logging

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-i",
        "--input",
        required=True,
        help="stats/ratio/by_motif/{sample}.genome.tsv",
    )
    ap.add_argument("-f", "--filtered", required=True, help="report_sites/filtered.tsv")
    ap.add_argument("-s", "--sample", required=True, help="sample name (p_<sample>)")
    ap.add_argument("-o", "--output", required=True, help="output TSV")
    ap.add_argument(
        "-b",
        "--base",
        default="A",
        help="target base of the 3-mer center (default A; T->U only for A)",
    )
    args = ap.parse_args()

    con = duckdb.connect()
    pcol = f"p_{args.sample}"
    # by_motif 3-mers are DNA; show RNA (T->U) only for A-target runs.
    to_rna = (
        "replace(upper(m), 'T', 'U')" if args.base.upper() == "A" else "upper(m)"
    )
    q = f"""
    WITH bm AS (
        SELECT upper("Motif") AS m,
               CAST("Count_all" AS BIGINT)      AS candidates,
               CAST("Depth" AS DOUBLE)          AS depth1,
               CAST("Unconverted" AS DOUBLE)    AS uncon1,
               CAST("Depth_all" AS DOUBLE)      AS depth_all,
               CAST("Unconverted_all" AS DOUBLE) AS uncon_all
        FROM read_csv('{args.input}', delim='\\t', header=true)
    ),
    f AS (
        SELECT upper(substr("Motif", 15, 3)) AS m, count(*) AS filtered
        FROM read_csv('{args.filtered}', delim='\\t', header=true)
        WHERE "{pcol}" < 1
        GROUP BY 1
    )
    SELECT {to_rna} AS motif,
           bm.candidates,
           f.filtered,
           f.filtered / bm.candidates * 1000.0 AS enrichment,
           CASE WHEN bm.depth1 > 0
                THEN 100.0 * (bm.depth1 - bm.uncon1) / bm.depth1 ELSE 0.0 END
                AS conv_hq,
           CASE WHEN bm.depth_all > 0
                THEN 100.0 * (bm.depth_all - bm.uncon_all) / bm.depth_all
                ELSE 0.0 END
                AS conv_all
    FROM bm
    JOIN f USING (m)
    WHERE bm.candidates > 0
    ORDER BY enrichment DESC, f.filtered DESC, motif
    """
    rows = con.execute(q).fetchall()

    with open(args.output, "w") as fh:
        fh.write("Motif\tCandidates\tFiltered\tEnrichment\tConvHQ\tConvAll\n")
        for motif, cand, filt, enr, hq, al in rows:
            fh.write(
                f"{motif}\t{cand}\t{filt}\t{enr:.4f}\t{hq:.2f}\t{al:.2f}\n"
            )
    logging.info(
        f"Wrote {len(rows)} motifs (sample {args.sample}) -> {args.output}"
    )


if __name__ == "__main__":
    main()
