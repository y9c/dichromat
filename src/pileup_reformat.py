#!/usr/bin/env python3
"""Rewrite countmut >= 0.2.0 per-base composition output into the legacy pileup layout.

countmut 0.2.0 removed the tiered conversion view (``--ref-base``/``--mut-base``,
columns ``u0..u2/m0..m2``) in favor of a single per-base composition table:

    chrom  pos  strand  ref  depth  a  c  g  t  n

The downstream pileup consumers (``unfilter_genes_stat``,
``motif_conversion_rate_stat``, ``merge_samples.py``, ``filter_sites.py``)
expect the legacy 10-column layout:

    chrom  pos  strand  motif  u0  u1  u2  m0  m1  m2

This script rewrites the composition rows into that layout:

  * rows are kept only where the reference base equals ``--ref-base``
    (the legacy tool emitted target sites only) and the reference-base +
    mutation-base counts are > 0 (legacy ``u1+m1+u2+m2 > 0`` row filter);
  * ``u1`` = read bases equal to the reference base (default A),
    ``m1`` = read bases equal to the mutation base (default G);
    ``u0/u2/m0/m2`` = 0 (countmut >= 0.2 does not classify reads into
    quality/conversion tiers; all accepted reads are counted, so the
    per-site ratio is computed over a looser read set than countmut 0.0.x);
  * ``motif`` = (2*pad+1)-base window around the site from the reference
    (default pad=15 -> 31-mer with the site as the 16th character),
    N-padded at contig ends and reverse-complemented on the minus strand
    (legacy behavior).

``pos`` is 1-based and strands are ``+``/``-`` in both input and output.
The input must be the built-in composition table of ``countmut -o``
(per-strand; do not pass ``--strandless``/``--count-indels``).

Usage:
  pileup_reformat.py -i composition.tsv -r ref.fa --ref-base A --mut-base G -o pileup.tsv
"""

from __future__ import annotations

import argparse
import logging
import os

import pysam

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pileup_reformat")

_RC = str.maketrans("ACGTNacgtn", "TGCANtgcna")
_HEADER = "chrom\tpos\tstrand\tmotif\tu0\tu1\tu2\tm0\tm1\tm2"


def revcomp(s: str) -> str:
    return s.translate(_RC)[::-1]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("-i", "--input", required=True, help="countmut composition TSV")
    ap.add_argument("-r", "--ref", required=True, help="reference FASTA")
    ap.add_argument("--ref-base", default="A", help="reference base (default A)")
    ap.add_argument("--mut-base", default="G", help="mutation base (default G)")
    ap.add_argument(
        "--pad", type=int, default=15, help="motif half-window (default 15)"
    )
    ap.add_argument("-o", "--output", required=True, help="legacy-layout TSV output")
    args = ap.parse_args()

    ref_base = args.ref_base.upper()
    mut_base = args.mut_base.upper()
    if ref_base not in "ACGT" or mut_base not in "ACGT" or ref_base == mut_base:
        ap.error("--ref-base/--mut-base must be distinct letters in ACGT")
    if args.pad < 0:
        ap.error("--pad must be >= 0")
    width = 2 * args.pad + 1

    if not os.path.exists(args.ref + ".fai"):
        pysam.faidx(args.ref)
    fa = pysam.FastaFile(args.ref)

    with open(args.input, newline="") as fin, open(args.output, "w") as fout:
        fout.write(_HEADER + "\n")
        header = fin.readline().rstrip("\n").split("\t")
        col = {name: i for i, name in enumerate(header)}
        for need in ("chrom", "pos", "strand", "ref",
                     ref_base.lower(), mut_base.lower()):
            if need not in col:
                raise SystemExit(
                    f"pileup_reformat: missing column {need!r} in {args.input} "
                    "(expected a countmut >= 0.2.0 composition table: "
                    "chrom pos strand ref depth a c g t n)"
                )

        cur_chrom: str | None = None
        cur_seq: str | None = None
        rows_in = rows_out = 0
        for line in fin:
            f = line.rstrip("\n").split("\t")
            rows_in += 1
            if f[col["ref"]].upper() != ref_base:
                continue
            u = int(f[col[ref_base.lower()]] or 0)
            m = int(f[col[mut_base.lower()]] or 0)
            if u + m == 0:
                continue
            chrom = f[col["chrom"]]
            pos = int(f[col["pos"]])
            strand = f[col["strand"]]
            if chrom != cur_chrom:
                cur_chrom = chrom
                cur_seq = fa[chrom] if chrom in fa.references else None
                if cur_seq is None:
                    log.warning("contig %r not in %s; motif set to N", chrom, args.ref)
            motif = "N" * width
            if cur_seq is not None:
                i0 = pos - 1
                if 0 <= i0 < len(cur_seq):
                    start = i0 - args.pad
                    end = i0 + args.pad + 1
                    if start < 0:
                        motif = "N" * (-start) + cur_seq[: min(end, len(cur_seq))]
                    elif end > len(cur_seq):
                        motif = cur_seq[start:] + "N" * (end - len(cur_seq))
                    else:
                        motif = cur_seq[start:end].upper()
            if strand == "-":
                motif = revcomp(motif)
            fout.write(f"{chrom}\t{pos}\t{strand}\t{motif}\t0\t{u}\t0\t0\t{m}\t0\n")
            rows_out += 1
    fa.close()
    log.info(
        "rewrote %d/%d rows (ref==%s, %s+%s>0) -> %s",
        rows_out, rows_in, ref_base, ref_base.lower(), mut_base.lower(), args.output,
    )


if __name__ == "__main__":
    main()
