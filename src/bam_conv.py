#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2024 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Created: 2024-02-18 03:13

""">probe_0 probe_0 123456789012345678
TATCTGTCTCGACGTNNANNGGCCTTTGCAACTAGAATTACACCATAATTGCT.

>probe_25 probe_25
TATCTGTCTCGACGTNNANNGGCATTCAAGCCTAGAATTACACCATAATTGCT
>probe_50 probe_50
TATCTGTCTCGACGTNNANNGGCGAGGTGATCTAGAATTACACCATAATTGCT
>probe_75 probe_75
TATCTGTCTCGACGTNNANNGGCTTCAACAACTAGAATTACACCATAATTGCT
>probe_100 probe_100
TATCTGTCTCGACGTNNANNGGCGATGGTTTCTAGAATTACACCATAATTGCT
"""

import multiprocessing as mp
from collections import Counter, defaultdict

import pysam


def parse_chrom(input_file, chrom):
    signal_counter = defaultdict(Counter)
    background_counter = defaultdict(Counter)
    samfile = pysam.AlignmentFile(input_file, "rb")
    for read in samfile.fetch(chrom, until_eof=True):
        c = read.get_tag("Yf")
        u = read.get_tag("Zf")
        # if c + u < 5 or u > 3:
        # or u > 2:
        if c is None or u is None or int(c) + int(u) < 5:
            continue
        for read_pos, ref_pos, ref_base in read.get_aligned_pairs(
            with_seq=True, matches_only=True
        ):
            read_base = read.query_sequence[read_pos]
            ref_base = ref_base.upper()
            rate_str = read.reference_name.split("_")[-1]
            if rate_str.isdigit():
                rate = int(rate_str)
            else:
                rate = 0
            # rate = read.reference_name.split("-")[-1]
            if ref_pos + 1 == 18:
                # if ref_pos + 1 in [5486, 12141]:
                signal_counter[rate][(ref_base, read_base)] += 1
            else:
                background_counter[rate][(ref_base, read_base)] += 1
    samfile.close()
    return signal_counter, background_counter


def parse_bam(input_file, ref_suffix="probe_", threads=16):
    signal_counter = defaultdict(Counter)
    background_counter = defaultdict(Counter)
    samfile = pysam.AlignmentFile(input_file, "rb")
    refs = [r for r in samfile.references if r.startswith(ref_suffix)]
    samfile.close()
    with mp.Pool(threads) as pool:
        for s, b in pool.starmap(parse_chrom, [(input_file, r) for r in refs]):
            for k, v in s.items():
                signal_counter[k] += v
            for k, v in b.items():
                background_counter[k] += v
    return signal_counter, background_counter


if __name__ == "__main__":
    import argparse

    args = argparse.ArgumentParser()
    # list of iinput files
    args.add_argument("input_file", nargs="+")
    args.add_argument("-t", "--threads", type=int, default=16)
    args.add_argument("-r", "--ref_suffix", type=str, default="probe_")
    args = args.parse_args()

    # input_file = "10-54-1.genes.bam"

    print("Sample", "Rate", "m6A%", "Aconvert%", "Cconvert%", sep="\t")
    for input_file in args.input_file:
        signal_counter, background_counter = parse_bam(
            input_file, ref_suffix=args.ref_suffix, threads=args.threads
        )
        for rate, b in background_counter.items():
            res = [rate]
            s = signal_counter.get(rate, Counter())
            # A -> A and A -> G
            unc = s[("A", "A")]
            con = s[("A", "G")]
            if con + unc > 0:
                res.append(f"{unc/(con + unc):.3%}")
            else:
                res.append("NA")
            # A -> A and A -> G
            unc = b[("A", "A")]
            con = b[("A", "G")]
            res.append(f"{con/(con + unc):.3%}")
            # C -> C and C -> T
            unc = b[("C", "C")]
            con = b[("C", "T")]
            res.append(f"{con/(con + unc):.3%}")
            print(input_file.split("/")[-1].rsplit(".", 2)[0], *res, sep="\t")
