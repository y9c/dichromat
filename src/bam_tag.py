#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2023 Ye Chang yech1990@gmail.com
# Distributed under terms of the GNU license.
#
# Created: 2023-01-30 21:52

"""Add tag into calmd bam."""

import logging
import os
import tempfile
from multiprocessing import Pool

import pysam

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def parse_read(read, strand_type):
    if read.is_unmapped:
        return read

    # if not unstranded, need to set strand based on the read
    # else just the YZ tag
    if strand_type != 0:
        if read.is_paired:
            if read.is_unmapped or read.mate_is_unmapped:
                return read
            strand = "-" if read.is_read1 is read.is_reverse else "+"
        else:
            if read.is_unmapped:
                return read
            strand = "-" if read.is_reverse else "+"

        # library type forward strand (Swift kit, if not, need to change this code)
        # changed into revser strand
        if strand_type == -1:
            strand = "-" if strand == "+" else "+"
    else:
        strand = read.get_tag("YZ") if read.has_tag("YZ") else "+"

    s = read.query_sequence

    if strand == "+":
        r0, a0 = "A", "G"
        r1, a1 = "C", "T"
    else:
        r0, a0 = "T", "C"
        r1, a1 = "G", "A"

    # Yf:i:<N>: Number of conversions are detected in the read.
    # Zf:i:<N>: Number of un-converted bases are detected in the read. Yf + Zf = total number of bases which can be converted in the read sequence.
    # YZ:A:<A>: The value + or – indicate the read is mapped to REF-3N (+) or REF-RC-3N (-).
    yf = 0
    zf = 0
    yc = 0
    zc = 0
    # Number of submsitution
    ns = 0
    # Number of CLIP and INDEL
    nc = 0

    for read_pos, _, ref_base in read.get_aligned_pairs(with_seq=True):
        # Dn not forget to convert the ref_base to upper case
        ref_base = ref_base.upper() if ref_base is not None else None
        read_base = s[read_pos] if read_pos is not None else None

        if ref_base == r0:
            if read_base == a0:
                yf += 1
            elif read_base == r0:
                zf += 1
            elif read_base is not None:
                ns += 1
        elif ref_base == r1:
            if read_base == a1:
                yc += 1
            elif read_base == r1:
                zc += 1
            elif read_base is not None:
                ns += 1
        elif ref_base is not None:
            if read_base is not None and ref_base != read_base:
                ns += 1
        else:
            if read_base is not None:
                nc += 1

    read.set_tag("Yf", yf)
    read.set_tag("Zf", zf)
    read.set_tag("Yc", yc)
    read.set_tag("Zc", zc)
    read.set_tag("NS", ns)
    read.set_tag("NC", nc)
    # should mark file in the mapping, so read strand can be tell
    read.set_tag("YZ", strand, value_type="A")
    # yf, zf, yc, zc, ns, nc, strand
    return read


def add_tag(bam, output, strand_type):
    logging.info(f"Add tag into bam for {bam}")
    bamfile = pysam.AlignmentFile(bam, "rb")
    bamout = pysam.AlignmentFile(output, "wb", template=bamfile)
    for read in bamfile:
        # modified read tag in place
        parse_read(read, strand_type=strand_type)
        bamout.write(read)

    bamfile.close()
    bamout.close()
    logging.info(f"Finished add tag into bam for {bam}")


def primary_group(group, drop_reverse):
    # NS: Number of submsitution
    # NC: number of soft clip
    clip_weight = 0.2
    # single end penalty weight
    se_weight = 1.5

    # each group are mate pair that multiple mapped, so the query_name is same
    # seperate group by mate pair, and store into list of list
    # if is single end, keep one item in the list
    # if is pair end, keep two item in the list
    paired_group = {}
    for r in group:
        if r.is_paired:
            # and r.is_primary:
            k = (
                r.reference_name,
                (
                    r.reference_start // 10
                    if r.is_read1
                    else r.next_reference_start // 10
                ),
            )
            if r.is_read1:
                if k in paired_group:
                    paired_group[k][0] = r
                else:
                    paired_group[k] = [r, None]
            else:
                if k in paired_group:
                    paired_group[k][1] = r
                else:
                    paired_group[k] = [None, r]
        else:
            paired_group[(r.reference_name, r.reference_start)] = [r]

    score_group = {}
    for k, v in paired_group.items():
        v = [x for x in v if x is not None]
        if len(v) == 2:
            if drop_reverse and (
                v[0].get_tag("YZ") == "-" or v[1].get_tag("YZ") == "-"
            ):
                score_group[k] = float("inf")
            elif v[0].is_unmapped or v[1].is_unmapped:
                score_group[k] = float("inf")
            else:
                score_group[k] = round(
                    (
                        v[0].get_tag("NS")
                        + v[0].get_tag("NC") * clip_weight
                        + v[1].get_tag("NS")
                        + v[1].get_tag("NC") * clip_weight
                    )
                    / (v[0].query_alignment_length + v[1].query_alignment_length),
                    2,
                )
        else:
            if drop_reverse and v[0].get_tag("YZ") == "-":
                score_group[k] = float("inf")
            elif v[0].is_unmapped:
                score_group[k] = float("inf")
            else:
                score_group[k] = round(
                    (
                        (v[0].get_tag("NS") + v[0].get_tag("NC") * clip_weight)
                        * se_weight
                        / v[0].query_alignment_length
                    ),
                    2,
                )

    # make it secondary
    # read.flag |= 256
    # make it primary
    # read.flag &= ~256
    k2s = sorted(score_group.items(), key=lambda x: x[1])
    for i, (k, s) in enumerate(k2s):
        for r in paired_group[k]:
            if r:
                r.set_tag("AP", s)
                if i == 0:
                    r.flag &= ~256
                else:
                    r.flag |= 256
                yield r


def primary_bam(input_sam_file, output_sam_file, max_score, drop_reverse):
    logging.info(f"Select primary of bam for {input_sam_file}")
    input_sam = pysam.AlignmentFile(input_sam_file, "rb")
    output_sam = pysam.AlignmentFile(output_sam_file, "wb", template=input_sam)

    group_name = ""
    group = []
    for read in input_sam.fetch(until_eof=True):
        if read.query_name != group_name:
            # write output
            if len(group) > 0:
                for r in primary_group(group, drop_reverse=drop_reverse):
                    if r.get_tag("AP") <= max_score:
                        output_sam.write(r)

            group_name = read.query_name
            group = [read]
        else:
            group.append(read)

    # write output
    if len(group) > 0:
        for r in primary_group(group, drop_reverse=drop_reverse):
            if r.get_tag("AP") <= max_score:
                output_sam.write(r)

    output_sam.close()
    input_sam.close()
    logging.info(f"Finished set primary into bam for {input_sam_file}")


# parallel version for unsorted bam


def split_bam_into_chunk(bam, chunk_size=10_000_000, threads=1):
    output_bam_list = []

    bamfile = pysam.AlignmentFile(bam, "rb", threads=threads)
    output_bam = tempfile.NamedTemporaryFile(
        suffix=".bam", delete=False, delete_on_close=False
    )
    output_bam_list.append(output_bam.name)
    chunk = 0
    reads_in_this_chunk = 0
    old_name = None
    outfile = pysam.AlignmentFile(
        output_bam.name, "wb", template=bamfile, threads=threads
    )

    for read in bamfile.fetch(until_eof=True):
        if old_name != read.query_name and reads_in_this_chunk > chunk_size:
            reads_in_this_chunk = 0
            chunk += 1
            outfile.close()
            output_bam = tempfile.NamedTemporaryFile(
                suffix=".bam", delete=False, delete_on_close=False
            )
            output_bam_list.append(output_bam.name)
            outfile = pysam.AlignmentFile(
                output_bam.name, "wb", template=bamfile, threads=threads
            )

        outfile.write(read)
        old_name = read.query_name
        reads_in_this_chunk += 1
    outfile.close()
    return output_bam_list


def add_tag_and_mark_primary_by_chunk(
    input_bam,
    output_bam,
    max_score,
    strand_type,
    drop_reverse=True,
    chunk_size=1_000_000,
    threads=8,
):
    logging.info("Split bam into chunk")
    bam_list = split_bam_into_chunk(input_bam, chunk_size=chunk_size, threads=threads)
    logging.info("Finished split bam into chunk")

    with Pool(threads) as p:
        p.starmap(add_tag, [(bam, bam + ".tag", strand_type) for bam in bam_list])
    for bam in bam_list:
        os.remove(bam)

    with Pool(threads) as p:
        p.starmap(
            primary_bam,
            [
                (bam + ".tag", bam + ".primary", max_score, drop_reverse)
                for bam in bam_list
            ],
        )
    for bam in bam_list:
        os.remove(bam + ".tag")

    logging.info("Merge bam")
    bamin = pysam.AlignmentFile(input_bam, "rb")
    bamout = pysam.AlignmentFile(output_bam, "wb", template=bamin)
    for bam in bam_list:
        bamfile = pysam.AlignmentFile(bam + ".primary", "rb")
        for read in bamfile:
            bamout.write(read)
        bamfile.close()
        os.remove(bam + ".primary")
        logging.info(f"Finished merge {bam}")
    bamin.close()
    bamout.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Add tag")
    parser.add_argument("input_bam", help="bam file")
    parser.add_argument("output_bam", help="output bam file")
    parser.add_argument(
        "-s",
        "--max-score",
        type=float,
        default=float("inf"),
        help="max score for primary",
    )
    parser.add_argument(
        "-w",
        "--window-size",
        type=int,
        default=100_000,
        help="window size for split bam",
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        type=int,
        default=2_000_000,
        help="chunk size for split bam",
    )
    parser.add_argument(
        "-t", "--threads", type=int, default=8, help="number of threads"
    )
    # parser.add_argument(
    #     "--reversed-lib", action="store_true", help="library type is reversed"
    # )
    # parser.add_argument(
    #     "--forward-lib",
    #     action="store_true",
    #     default=False,
    #     help="library type is forward",
    # )
    parser.add_argument(
        "-S",
        "--strand-type",
        choices=["-1", "0", "1"],
        default="1",
        help="strand type, 1 for forward, -1 for reverse, 0 for unstranded",
    )
    parser.add_argument("--drop-reverse", action="store_true", help="drop reverse read")

    args = parser.parse_args()
    # add_tag(args.input_bam, args.output_bam)
    add_tag_and_mark_primary_by_chunk(
        args.input_bam,
        args.output_bam,
        max_score=args.max_score,
        strand_type=int(args.strand_type),
        drop_reverse=args.drop_reverse,
        chunk_size=args.chunk_size,
        threads=args.threads,
    )
