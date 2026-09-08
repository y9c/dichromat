#!/usr/bin/env python3
"""report_sites.py: render all site-related report sections into one sites.html.

Sections rendered (via report_html.py modes):
  * site table        (tables)   from mqc_sites stats
  * metagene          (metagene) from sites.tsv.gz + GTF
  * sequence logo     (logo)     from sites.tsv.gz
  * motif conversion  (motifconv) per sample x reftype
  * motif enrichment  (motiffig)  per sample

Usage:
  report_sites.py OUT.html \
    --mqc f1.tsv f2.tsv ... \
    --motif-ratio r1.tsv r2.tsv ... \
    --sites sites.tsv.gz --gtf gtf \
    --by-motif m1.tsv m2.tsv ... \
    --by-motif-genome g1.tsv g2.tsv ... \
    --filtered filtered.tsv \
    --samples s1 s2 ...
"""

import argparse
import os
import subprocess
import sys
import tempfile

REPORT_HTML = os.environ.get("REPORT_HTML", "report_html")
CORALSNAKE = os.environ.get("CORALSNAKE", "coralsnake")
MOTIF_ENRICH = os.environ.get("MOTIF_ENRICH", "motif_enrich")


def run(cmd):
    subprocess.run(cmd, shell=True, check=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("output")
    ap.add_argument("--mqc", nargs="+", required=True)
    ap.add_argument("--motif-ratio", nargs="+", default=[])
    ap.add_argument("--sites", required=True)
    ap.add_argument("--gtf", required=True)
    ap.add_argument("--by-motif", nargs="+", default=[])
    ap.add_argument("--by-motif-genome", nargs="+", default=[])
    ap.add_argument("--filtered", required=True)
    ap.add_argument("--samples", nargs="+", required=True)
    ap.add_argument("--reftypes", nargs="+", default=[])
    args = ap.parse_args()

    tmpdir = tempfile.mkdtemp(prefix="report_sites_")
    try:
        sections = []

        # Detect whether the sites table has any data rows (metagene/logo need
        # at least one site; coralsnake errors on an empty table).
        import gzip
        _has_sites = False
        with gzip.open(args.sites, "rt") as _fh:
            next(_fh, None)  # skip header
            for _line in _fh:
                if _line.strip():
                    _has_sites = True
                    break

        # 1. site table
        table_html = os.path.join(tmpdir, "table.html")
        run(f"{REPORT_HTML} tables {table_html} " + " ".join(args.mqc)
            + " " + " ".join(args.motif_ratio))
        sections.append(table_html)

        # 2. metagene (skip if no sites; coralsnake errors on an empty table)
        if _has_sites:
            prof = os.path.join(tmpdir, "metagene.tsv")
            run(f"{CORALSNAKE} metagene -i {args.sites} -g {args.gtf} -H "
                f"--meta-columns 1,2,3 --bins 100 --export-profile {prof}")
            meta_html = os.path.join(tmpdir, "metagene.html")
            run(f"{REPORT_HTML} metagene {meta_html} {prof}")
            sections.append(meta_html)

        # 3. sequence logo (skip if no sites)
        if _has_sites:
            logo_html = os.path.join(tmpdir, "logo.html")
            run(f"zcat {args.sites} | awk -F '\\t' 'NR==1{{for(i=7;i<=NF;i++) "
                f"if($$i ~ /^Depth_/) d[i]=1; next}} "
                f"{{s=0; for(i in d) s+=$$i; if($$6 ~ /^[ACGTUNn]+$/ && s>0) "
                f"print $$6 \"\\t\" s}}' | {CORALSNAKE} logo -i - --matrix {logo_html}")
            sections.append(logo_html)

        # 4. per-sample motif conversion + enrichment (skip if no motif data)
        _has_motif = False
        for _mf in args.by_motif:
            with open(_mf) as _fh:
                next(_fh, None)  # skip header
                for _line in _fh:
                    if _line.strip():
                        _has_motif = True
                        break
            if _has_motif:
                break
        if _has_motif:
            for si, sample in enumerate(args.samples):
                for reftype in args.reftypes:
                    motif_html = os.path.join(tmpdir, f"motif_{sample}_{reftype}.html")
                    run(f"{REPORT_HTML} motifconv {motif_html} " + " ".join(args.by_motif))
                    sections.append(motif_html)
            if si < len(args.by_motif_genome):
                enrich_tsv = os.path.join(tmpdir, f"enrich_{sample}.tsv")
                run(f"{MOTIF_ENRICH} -i {args.by_motif_genome[si]} -f {args.filtered} "
                    f"-s {sample} -o {enrich_tsv}")
                enrich_html = os.path.join(tmpdir, f"enrich_{sample}.html")
                run(f"{REPORT_HTML} motiffig {enrich_html} {enrich_tsv} {sample}")
                sections.append(enrich_html)

        # 5. assemble
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        run(f"{REPORT_HTML} assemble {args.output} " + " ".join(sections))
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)
    print(f"[report_sites] wrote {args.output}")


if __name__ == "__main__":
    sys.exit(main())