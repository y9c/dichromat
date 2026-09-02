from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
import os
import yaml

# Load default config
with open(Path(workflow.basedir) / "default.yaml") as f:
    merged_config = yaml.safe_load(f)

# Merge with user config (deep merge for 'path' dictionary)
# We store user overrides first
user_config = dict(config)
user_path = user_config.get("path", {})

# Apply user overrides to merged_config
for k, v in user_config.items():
    if k == "path" and isinstance(v, dict):
        merged_config["path"].update(v)
    elif k == "reference" and isinstance(v, dict):
        merged_config.setdefault("reference", {}).update(v)
    else:
        merged_config[k] = v

config = merged_config

# Flatten reference into top-level config for backward compatibility with rules
if "reference" in config:
    for k, v in config["reference"].items():
        if k not in config:
            config[k] = v

# Determine BATCH name
BATCH = config.get("batch", "dichromat_run")
IS_ETAM = config.get("is_etam", "eTAM" in BATCH)
SKIP_SAMPLES = config.get("skip_samples", [])

# Detect if we are running inside the dichromat container
INSIDE_CONTAINER = os.environ.get("PIPELINE_HOME") == "/pipeline"

# If running in a container, we should use the default tool names
# which are correctly set up in the container's PATH.
if config.get("container") or INSIDE_CONTAINER:
    # Revert 'path' to defaults if using a container
    with open(Path(workflow.basedir) / "default.yaml") as f:
        clean_defaults = yaml.safe_load(f)
        config["path"] = clean_defaults.get("path", {})


# Resolve container path to absolute if relative
CONTAINER = config.get("container")
if CONTAINER and not os.path.isabs(CONTAINER):
    CONTAINER = os.path.normpath(os.path.join(workflow.basedir, CONTAINER))


# Container directive for rules
# If already inside the container, we MUST set this to None to avoid nesting
container: None if INSIDE_CONTAINER else CONTAINER


def resolve_config_path(p):
    if not p or not isinstance(p, str) or os.path.isabs(p):
        return p
    p = os.path.expanduser(p)
    # 1. Try relative to CWD (User workspace or where they ran the command)
    if os.path.exists(p):
        return os.path.abspath(p)
    # 2. Try relative to project root (passed from dichromat.sh or Snakefile dir)
    base = config.get("project_dir", workflow.basedir)
    p_joined = os.path.join(base, p)
    if os.path.exists(p_joined):
        return os.path.normpath(p_joined)
    # 3. Fallback to abspath from CWD
    return os.path.abspath(p)


REF = config.get("reference", {})
# Expand user paths and resolve relative paths in REF dictionary
for ref_type in REF:
    if isinstance(REF[ref_type], dict):
        for key, val in REF[ref_type].items():
            REF[ref_type][key] = resolve_config_path(val)
    elif isinstance(REF[ref_type], list):
        REF[ref_type] = [resolve_config_path(f) for f in REF[ref_type]]
    elif isinstance(REF[ref_type], str):
        REF[ref_type] = resolve_config_path(REF[ref_type])


TEMPDIR = Path(config.get("tempdir", ".tmp"))
# Convert PATH dict to SimpleNamespace for dot notation access (e.g., PATH.python instead of PATH['python'])
PATH = SimpleNamespace(**config.get("path", {}))

INTERNALDIR = Path("internal_files")
BENCHDIR = Path(".snakemake/benchmarks")
MARKDUP = config.get("markdup", True)
SPLICE_GENOME = config.get("splice_genome", True)
SPLICE_CONTAM = config.get("splice_contamination", False)


wildcard_constraints:
    sample=r"[^/\.]+",
    rn=r"run[0-9]+",
    reftype="genome|transcript|genes|contamination",
    libmode="PE|SE",


SAMPLE2DATA = defaultdict(lambda: defaultdict(dict))
GROUP2SAMPLE = defaultdict(list)
SAMPLE2LIB = defaultdict(str)
SAMPLE2ADP = defaultdict(str)

# Support both 'samples' and 'samples_{BATCH}' for compatibility
samples_dict = config.get("samples") or config.get(f"samples_{BATCH}")
if not samples_dict:
    raise SystemExit(f"Please add 'samples' or 'samples_{BATCH}' in your config file")

for s, v in samples_dict.items():
    s = str(s)
    SAMPLE2LIB[s] = v.get("libtype", config.get("libtype", ""))  # Built-in Name
    SAMPLE2ADP[s] = v.get("adapter", config.get("adapter", ""))  # Custom Sequence
    if "group" in v:
        GROUP2SAMPLE[v["group"]].append(s)
    for i, v2 in enumerate(v["data"], 1):
        r = f"run{i}"
        SAMPLE2DATA[str(s)][r] = {
            k: os.path.expanduser(v3) for k, v3 in dict(v2).items()
        }

HAS_GENES = bool(REF.get("genes"))
HAS_CONTAM = bool(REF.get("contamination"))

REFTYPES = (
    (["contamination"] if HAS_CONTAM else [])
    + (["genes"] if HAS_GENES else [])
    + ["transcript", "genome"]
)


def is_pe(sample, rn):
    return len(SAMPLE2DATA[sample][rn]) == 2


def get_lib_subdir(sample, rn):
    return "PE" if is_pe(sample, rn) else "SE"


rule all:
    input:
        "report_reads/mapping.html",
        "report_reads/trimmed.html",
        "report_reads/unmapped.html",
        "report_sites/sites.html",
        "report_sites/filtered.tsv" if IS_ETAM else "report_sites/sites.tsv.gz",
        expand("report_sites/grouped/{group}.parquet", group=GROUP2SAMPLE.keys()),
        [
            INTERNALDIR / f"fastq/tooshort/{sample}_{rn}_{rd}.fq.gz"
            for sample, v in SAMPLE2DATA.items()
            for rn, v2 in v.items()
            for rd in v2.keys()
        ],
        INTERNALDIR / "README.md",
        INTERNALDIR / "stats/ratio/probe.tsv" if HAS_GENES else [],
    benchmark:
        BENCHDIR / "all.benchmark.txt"


# prepare ref


rule internal_readme:
    output:
        INTERNALDIR / "README.md",
    benchmark:
        BENCHDIR / "internal_readme.benchmark.txt"
    shell:
        """
        cat <<'EOF' > {output}
# Internal Pipeline Files

This directory contains intermediate files for the `dichromat` pipeline.

## Data Flow & Directory Structure

### 1. `qc/` & `fastq/`
- `qc/trimming/`: Trimming reports from `cutseq`.
- `qc/fastqc_trimmed/`: FastQC reports for trimmed reads.
- `qc/fastqc_unmapped/`: FastQC reports for unmapped reads.
- `fastq/tooshort/`: Reads that were too short after trimming.
- `fastq/unmapped/`: Reads that failed to map to any reference.

### 2. `ref/`
- Generated indices and processed reference files organized in subdirectories.

### 3. `bam/`
- `bam/per_run/`: Initial alignments for each sequencing run.
- `bam/*.genome.bam`: Final merged, deduplicated, and sorted BAM aligned to genome.
- `bam/*.transcript.bam`: Aligned to the transcriptome.

### 4. `stats/`
- `stats/count/`: Read count throughput tables.
- `stats/dedup/`: Detailed logs from `markdup` deduplication.
- `stats/ratio/by_motif/`: Global conversion ratios grouped by 3-mer motifs.
- `stats/mqc/reads/`: Summaries for the Mapping report.
- `stats/mqc/sites/`: Summaries for the Site report.

### 5. `pileup/`
- `pileup/per_sample/`: Site-level data (tsv.gz) for each sample.
- `pileup/transcript.parquet`: Merged transcriptome pileup.
- `pileup/genome.parquet`: Merged genomic pileup.

---
*Note: For final results (including merged `sites.tsv.gz`), see `report_reads/` and `report_sites/`.*
EOF
        """


rule combine_contamination_fa:
    input:
        REF.get("contamination", []) if "contamination" in REF else [],
    output:
        fa=INTERNALDIR / "ref/contamination.fa",
        fai=INTERNALDIR / "ref/contamination.fa.fai",
    benchmark:
        BENCHDIR / "combine_contamination_fa.benchmark.txt"
    shell:
        """
        mkdir -p $(dirname {output.fa})
        cat {input} > {output.fa}
        {PATH.samtools} faidx {output.fa} --fai-idx {output.fai}
        """


rule build_contamination_hisat3n_index:
    input:
        INTERNALDIR / "ref/contamination.fa",
    output:
        INTERNALDIR / "ref/contamination/index.indexed",
    params:
        basechange=config.get("base_change", "A,G"),
        prefix=str(INTERNALDIR / "ref/contamination/index"),
    threads: 64
    benchmark:
        BENCHDIR / "build_contamination_hisat3n_index.benchmark.txt"
    shell:
        """
        mkdir -p $(dirname {params.prefix})
        rm -f {params.prefix}*.ht2
        {PATH.hisat3n} build -p {threads} --base-change {params.basechange} {input} {params.prefix}
        touch {output}
        """


rule combine_genes_fa:
    input:
        REF.get("genes", []) if "genes" in REF else [],
    output:
        fa=INTERNALDIR / "ref/genes.fa",
        fai=INTERNALDIR / "ref/genes.fa.fai",
    benchmark:
        BENCHDIR / "combine_genes_fa.benchmark.txt"
    shell:
        """
        mkdir -p $(dirname {output.fa})
        cat {input} > {output.fa}
        {PATH.samtools} faidx {output.fa} --fai-idx {output.fai}
        """


rule prepared_transcript_ref:
    input:
        fa=REF["genome"]["fa"],
        gtf=REF["genome"]["gtf"],
    output:
        info=INTERNALDIR / "ref/transcript.tsv",
        seq=INTERNALDIR / "ref/transcript.fa",
    benchmark:
        BENCHDIR / "prepared_transcript_ref.benchmark.txt"
    shell:
        """
        mkdir -p $(dirname {output.info})
        {PATH.coralsnake} prepare -g {input.gtf} -f {input.fa} -o {output.info} -s {output.seq} -c -n -x -t -z
        """


# cut adapters


rule trim_se:
    input:
        lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R1") or [],
    output:
        c=temp(TEMPDIR / "trim/SE/{sample}_{rn}_R1.fq.gz"),
        s=temp(TEMPDIR / "trim/SE/{sample}_{rn}_tooshort_R1.fq.gz"),
        report=temp(TEMPDIR / "trim/SE/{sample}_{rn}_mqc.tsv"),
    params:
        minlen=config.get("min_len", 20),
        cut=lambda wildcards: (
            f"-A '{SAMPLE2LIB[wildcards.sample]}'"
            if SAMPLE2LIB[wildcards.sample]
            else f"-a '{SAMPLE2ADP[wildcards.sample]}'"
        ),
    threads: 8
    benchmark:
        BENCHDIR / "trim_se_{sample}_{rn}.benchmark.txt"
    shell:
        """
        {PATH.cutseq} -t {threads} {params.cut} -m {params.minlen} --auto-rc -o {output.c} -s {output.s} --json-file {output.report} {input}
        """


rule trim_pe:
    input:
        r1=lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R1") or [],
        r2=lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R2") or [],
    output:
        c1=temp(TEMPDIR / "trim/PE/{sample}_{rn}_R1.fq.gz"),
        c2=temp(TEMPDIR / "trim/PE/{sample}_{rn}_R2.fq.gz"),
        s1=temp(TEMPDIR / "trim/PE/{sample}_{rn}_tooshort_R1.fq.gz"),
        s2=temp(TEMPDIR / "trim/PE/{sample}_{rn}_tooshort_R2.fq.gz"),
        report=temp(TEMPDIR / "trim/PE/{sample}_{rn}_mqc.tsv"),
    params:
        minlen=config.get("min_len", 20),
        cut=lambda wildcards: (
            f"-A '{SAMPLE2LIB[wildcards.sample]}'"
            if SAMPLE2LIB[wildcards.sample]
            else f"-a '{SAMPLE2ADP[wildcards.sample]}'"
        ),
    threads: 8
    benchmark:
        BENCHDIR / "trim_pe_{sample}_{rn}.benchmark.txt"
    shell:
        """
        {PATH.cutseq} -t {threads} {params.cut} -m {params.minlen} --auto-rc -o {output.c1} {output.c2} -s {output.s1} {output.s2} --json-file {output.report} {input.r1} {input.r2}
        """


rule finalize_trim_report:
    input:
        lambda wildcards: (
            TEMPDIR / f"trim/PE/{wildcards.sample}_{wildcards.rn}_mqc.tsv"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"trim/SE/{wildcards.sample}_{wildcards.rn}_mqc.tsv"
        ),
    output:
        INTERNALDIR / "qc/trimming/{sample}_{rn}_mqc.tsv",
    benchmark:
        BENCHDIR / "finalize_trim_report_{sample}_{rn}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule finalize_discarded_reads:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"trim/PE/{wildcards.sample}_{wildcards.rn}_tooshort_{wildcards.rd}.fq.gz"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR
            / f"trim/SE/{wildcards.sample}_{wildcards.rn}_tooshort_{wildcards.rd}.fq.gz"
        ),
    output:
        INTERNALDIR / "fastq/tooshort/{sample}_{rn}_{rd}.fq.gz",
    benchmark:
        BENCHDIR / "finalize_discarded_reads_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        "cp {input} {output}"


# trimmed part qc


rule qc_trimmed:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"trim/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}.fq.gz"
        ),
    output:
        html=INTERNALDIR / "qc/fastqc_trimmed/{sample}_{rn}_{rd}/fastqc_report.html",
        text=INTERNALDIR / "qc/fastqc_trimmed/{sample}_{rn}_{rd}/fastqc_data.txt",
        summary=INTERNALDIR / "qc/fastqc_trimmed/{sample}_{rn}_{rd}/summary.txt",
    params:
        lambda wildcards: INTERNALDIR
        / f"qc/fastqc_trimmed/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}",
    benchmark:
        BENCHDIR / "qc_trimmed_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        "{PATH.falco} -o {params} {input}"


rule report_qc_trimmed:
    input:
        [
            INTERNALDIR / f"qc/fastqc_trimmed/{sample}_{rn}_{rd}/fastqc_data.txt"
            for sample, v in SAMPLE2DATA.items()
            for rn, v2 in v.items()
            for rd in v2.keys()
        ],
    output:
        "report_reads/trimmed.html",
    benchmark:
        BENCHDIR / "report_qc_trimmed.benchmark.txt"
    shell:
        "{PATH.report_html} qc {output} {input}"


# premap to contamination


rule premap_align_pe:
    input:
        fq1=TEMPDIR / "trim/PE/{sample}_{rn}_R1.fq.gz",
        fq2=TEMPDIR / "trim/PE/{sample}_{rn}_R2.fq.gz",
        idx=INTERNALDIR / "ref/contamination/index.indexed",
    output:
        mapped=temp(TEMPDIR / "premap/PE/{sample}_{rn}.contam.bam"),
        unmapped=temp(TEMPDIR / "premap/PE/{sample}_{rn}.unmap.bam"),
        summary=temp(TEMPDIR / "premap/PE/{sample}_{rn}.summary"),
    params:
        index=str(INTERNALDIR / "ref/contamination/index"),
        basechange=config.get("base_change", "A,G"),
        directional=lambda wildcards: (
            ""
            if SAMPLE2LIB[wildcards.sample] == "UNSTRANDED"
            else "--directional-mapping"
        ),
        splice_args=(
            "--pen-noncansplice 20 --min-intronlen 20 --max-intronlen 20"
            if SPLICE_CONTAM
            else "--no-spliced-alignment"
        ),
        secondary_args=(
            f"--secondary-change {config['secondary_change']}"
            if config.get("secondary_change")
            else ""
        ),
    threads: 64
    benchmark:
        BENCHDIR / "premap_align_pe_{sample}_{rn}.benchmark.txt"
    shell:
        """
        set -eo pipefail
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -1 {input.fq1} -2 {input.fq2} --base-change {params.basechange} {params.secondary_args} {params.directional} {params.splice_args} \
            --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-2,-0.8 |\
            {PATH.samtools} view -@ {threads} -e 'flag.proper_pair && !flag.unmap && !flag.munmap && qlen-sclen >= 30 && [XM] * 15 < (qlen-sclen)' -O BAM -U {output.unmapped} -o {output.mapped}
        """


rule premap_align_se:
    input:
        fq=TEMPDIR / "trim/SE/{sample}_{rn}_R1.fq.gz",
        idx=INTERNALDIR / "ref/contamination/index.indexed",
    output:
        mapped=temp(TEMPDIR / "premap/SE/{sample}_{rn}.contam.bam"),
        unmapped=temp(TEMPDIR / "premap/SE/{sample}_{rn}.unmap.bam"),
        summary=temp(TEMPDIR / "premap/SE/{sample}_{rn}.summary"),
    params:
        index=str(INTERNALDIR / "ref/contamination/index"),
        basechange=config.get("base_change", "A,G"),
        directional=lambda wildcards: (
            ""
            if SAMPLE2LIB[wildcards.sample] == "UNSTRANDED"
            else "--directional-mapping"
        ),
        splice_args=(
            "--pen-noncansplice 20 --min-intronlen 20 --max-intronlen 20"
            if SPLICE_CONTAM
            else "--no-spliced-alignment"
        ),
        secondary_args=(
            f"--secondary-change {config['secondary_change']}"
            if config.get("secondary_change")
            else ""
        ),
    threads: 64
    benchmark:
        BENCHDIR / "premap_align_se_{sample}_{rn}.benchmark.txt"
    shell:
        """
        set -eo pipefail
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -U {input.fq} --base-change {params.basechange} {params.secondary_args} {params.directional} {params.splice_args} \
            --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-2,-0.8 |\
            {PATH.samtools} view -@ {threads} -e '!flag.unmap && qlen-sclen >= 30 && [XM] * 15 < qlen-sclen' -O BAM -U {output.unmapped} -o {output.mapped}
        """


rule finalize_premap_summary:
    input:
        lambda wildcards: (
            TEMPDIR / f"premap/PE/{wildcards.sample}_{wildcards.rn}.summary"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"premap/SE/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/premap/{sample}_{rn}.summary",
    benchmark:
        BENCHDIR / "finalize_premap_summary_{sample}_{rn}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule premap_fixmate:
    input:
        TEMPDIR / "premap/{libmode}/{sample}_{rn}.contam.bam",
    output:
        temp(TEMPDIR / "premap/{libmode}/{sample}_{rn}.fixmate.bam"),
    threads: 8
    benchmark:
        BENCHDIR / "premap_fixmate_{libmode}_{sample}_{rn}.benchmark.txt"
    shell:
        """
        if [ "{wildcards.libmode}" == "PE" ]; then
            {PATH.samtools} fixmate -@ {threads} -m -O BAM {input} {output}
        else
            cp {input} {output}
        fi
        """


rule finalize_premap_bam:
    input:
        lambda wildcards: (
            TEMPDIR / f"premap/PE/{wildcards.sample}_{wildcards.rn}.fixmate.bam"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"premap/SE/{wildcards.sample}_{wildcards.rn}.fixmate.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.contamination.bam",
    threads: 64
    priority: 4
    benchmark:
        BENCHDIR / "finalize_premap_bam_{sample}_{rn}.benchmark.txt"
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule premap_get_unmapped:
    input:
        un=TEMPDIR / "premap/{libmode}/{sample}_{rn}.unmap.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/premap/{libmode}/{sample}_{rn}_R1.fq.gz"),
        r2=temp(TEMPDIR / "unmapped/premap/{libmode}/{sample}_{rn}_R2.fq.gz"),
    benchmark:
        BENCHDIR / "premap_get_unmapped_{libmode}_{sample}_{rn}.benchmark.txt"
    shell:
        """
        if [ "{wildcards.libmode}" == "PE" ]; then
            {PATH.samtools} fastq -F 0x900 -1 {output.r1} -2 {output.r2} -0 /dev/null -s /dev/null -n {input}
        else
            {PATH.samtools} fastq -F 0x900 -0 {output.r1} -n {input}
            touch {output.r2}
        fi
        """


# main mapping step (genes and transcript simutaneously if genes provided, otherwise just transcript)


rule index_transcript:
    input:
        rf=INTERNALDIR / "ref/transcript.fa",
    output:
        idx=INTERNALDIR / "ref/transcript/index.indexed",
    threads: 64
    benchmark:
        BENCHDIR / "index_transcript.benchmark.txt"
    shell:
        """
        mkdir -p {INTERNALDIR}/ref/map_index
        {PATH.prismalign} map -s MK --index-only --index-dir {INTERNALDIR}/ref/map_index -r {input.rf} -t {threads}
        touch {output.idx}
        """


rule index_genes:
    input:
        rf=INTERNALDIR / "ref/genes.fa",
    output:
        idx=INTERNALDIR / "ref/genes/index.indexed",
    threads: 64
    benchmark:
        BENCHDIR / "index_genes.benchmark.txt"
    shell:
        """
        mkdir -p {INTERNALDIR}/ref/map_index
        {PATH.prismalign} map -s MK --index-only --index-dir {INTERNALDIR}/ref/map_index -r {input.rf} -t {threads}
        touch {output.idx}
        """


rule mainmap_align_pe:
    input:
        fq1=lambda wildcards: (
            TEMPDIR / f"unmapped/premap/PE/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
            if HAS_CONTAM
            else TEMPDIR / f"trim/PE/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
        ),
        fq2=lambda wildcards: (
            TEMPDIR / f"unmapped/premap/PE/{wildcards.sample}_{wildcards.rn}_R2.fq.gz"
            if HAS_CONTAM
            else TEMPDIR / f"trim/PE/{wildcards.sample}_{wildcards.rn}_R2.fq.gz"
        ),
        rf1=lambda wildcards: INTERNALDIR / "ref/genes.fa" if HAS_GENES else [],
        rf2=INTERNALDIR / "ref/transcript.fa",
        idx1=lambda wildcards: (
            [INTERNALDIR / "ref/genes/index.indexed"] if HAS_GENES else []
        ),
        idx2=INTERNALDIR / "ref/transcript/index.indexed",
    output:
        mp2=temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.transcript.bam"),
        um=temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.main.bam"),
        summary=temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.summary"),
        mp1=[temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.genes.bam")] if HAS_GENES else [],
    threads: 128
    benchmark:
        BENCHDIR / "mainmap_align_pe_{sample}_{rn}.benchmark.txt"
    params:
        genes_ref=lambda wildcards, input: f"-r {input.rf1}" if HAS_GENES else "",
        genes_out=lambda wildcards, output: f"-o {output.mp1}" if HAS_GENES else "",
    shell:
        """
        {PATH.prismalign} map \
            -s MK -t {threads} \
            --ref-strand fwd \
            {params.genes_ref} \
            -r {input.rf2} --index-dir {INTERNALDIR}/ref/map_index \
            -1 {input.fq1} -2 {input.fq2} \
            --min-mapping-ratio 0.8 \
            -m 6 \
            --max-conversion-rates 1.0,0.33 \
            --report {output.summary} \
            {params.genes_out} \
            -o {output.mp2} \
            -u {output.um}
        """


rule mainmap_align_se:
    input:
        fq=lambda wildcards: (
            TEMPDIR / f"unmapped/premap/SE/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
            if HAS_CONTAM
            else TEMPDIR / f"trim/SE/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
        ),
        rf1=lambda wildcards: INTERNALDIR / "ref/genes.fa" if HAS_GENES else [],
        rf2=INTERNALDIR / "ref/transcript.fa",
        idx1=lambda wildcards: (
            [INTERNALDIR / "ref/genes/index.indexed"] if HAS_GENES else []
        ),
        idx2=INTERNALDIR / "ref/transcript/index.indexed",
    output:
        mp2=temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.transcript.bam"),
        um=temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.main.bam"),
        summary=temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.summary"),
        mp1=[temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.genes.bam")] if HAS_GENES else [],
    threads: 64
    benchmark:
        BENCHDIR / "mainmap_align_se_{sample}_{rn}.benchmark.txt"
    params:
        genes_ref=lambda wildcards, input: f"-r {input.rf1}" if HAS_GENES else "",
        genes_out=lambda wildcards, output: f"-o {output.mp1}" if HAS_GENES else "",
    shell:
        """
        {PATH.prismalign} map \
            -s MK -t {threads} \
            --ref-strand fwd \
            {params.genes_ref} \
            -r {input.rf2} --index-dir {INTERNALDIR}/ref/map_index \
            -1 {input.fq} \
            --min-mapping-ratio 0.8 \
            -m 6 \
            --max-conversion-rates 1.0,0.33 \
            --report {output.summary} \
            {params.genes_out} \
            -o {output.mp2} \
            -u {output.um}
        """


rule finalize_mainmap_summary:
    input:
        lambda wildcards: (
            TEMPDIR / f"mainmap/PE/{wildcards.sample}_{wildcards.rn}.summary"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"mainmap/SE/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/mainmap/{sample}_{rn}.summary",
    benchmark:
        BENCHDIR / "finalize_mainmap_summary_{sample}_{rn}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule finalize_mainmap_genes_bam:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"mainmap/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.genes.bam"
            if HAS_GENES
            else []
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.genes.bam",
    threads: 64
    benchmark:
        BENCHDIR / "finalize_mainmap_genes_bam_{sample}_{rn}.benchmark.txt"
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule finalize_mainmap_transcript_bam:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"mainmap/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.transcript.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.transcript.bam",
    threads: 64
    benchmark:
        BENCHDIR / "finalize_mainmap_transcript_bam_{sample}_{rn}.benchmark.txt"
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule mainmap_get_unmapped_pe:
    input:
        un=TEMPDIR / "mainmap/PE/{sample}_{rn}.main.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R1.fq.gz"),
        r2=temp(TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R2.fq.gz"),
    benchmark:
        BENCHDIR / "mainmap_get_unmapped_pe_{sample}_{rn}.benchmark.txt"
    shell:
        """
        {PATH.samtools} fastq -F 0x900 -1 {output.r1} -2 {output.r2} -0 /dev/null -s /dev/null -n {input}
        """


rule mainmap_get_unmapped_se:
    input:
        un=TEMPDIR / "mainmap/SE/{sample}_{rn}.main.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/mainmap/SE/{sample}_{rn}_R1.fq.gz"),
    benchmark:
        BENCHDIR / "mainmap_get_unmapped_se_{sample}_{rn}.benchmark.txt"
    shell:
        """
        {PATH.samtools} fastq -F 0x900 -0 {output.r1} -n {input}
        """


# postmap to genome


rule remap_align_pe:
    input:
        fq1=TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R1.fq.gz",
        fq2=TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R2.fq.gz",
    output:
        bam=temp(TEMPDIR / "remap/PE/{sample}_{rn}.mapped.bam"),
        summary=temp(TEMPDIR / "remap/PE/{sample}_{rn}.summary"),
        report=temp(TEMPDIR / "remap/PE/{sample}_{rn}.report_mqc.tsv"),
        unmapped=temp(TEMPDIR / "remap/PE/{sample}_{rn}.final_unmap.bam"),
    params:
        index=REF["genome"]["hisat3n"],
        basechange=config.get("base_change", "A,G"),
        directional=lambda wildcards: (
            ""
            if SAMPLE2LIB[wildcards.sample] == "UNSTRANDED"
            else "--directional-mapping"
        ),
        splice_args=(
            "--pen-noncansplice 20 --min-intronlen 20 --max-intronlen 20"
            if SPLICE_GENOME
            else "--no-spliced-alignment"
        ),
        secondary_args=(
            f"--secondary-change {config['secondary_change']}"
            if config.get("secondary_change")
            else ""
        ),
    threads: 64
    benchmark:
        BENCHDIR / "remap_align_pe_{sample}_{rn}.benchmark.txt"
    shell:
        """
        set -eo pipefail
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -1 {input.fq1} -2 {input.fq2} --base-change {params.basechange} {params.secondary_args} {params.directional} {params.splice_args} \
            --avoid-pseudogene --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-3,-0.5 |\
            {PATH.samtools} view -e '(([NS] + [NC]*0.2) / qlen) <= 0.08 && !flag.secondary' -@ {threads} -U {output.unmapped} --save-counts {output.report} -O BAM -o {output.bam}
        """


rule remap_align_se:
    input:
        fq=TEMPDIR / "unmapped/mainmap/SE/{sample}_{rn}_R1.fq.gz",
    output:
        bam=temp(TEMPDIR / "remap/SE/{sample}_{rn}.mapped.bam"),
        summary=temp(TEMPDIR / "remap/SE/{sample}_{rn}.summary"),
        report=temp(TEMPDIR / "remap/SE/{sample}_{rn}.report_mqc.tsv"),
        unmapped=temp(TEMPDIR / "remap/SE/{sample}_{rn}.final_unmap.bam"),
    params:
        index=REF["genome"]["hisat3n"],
        basechange=config.get("base_change", "A,G"),
        directional=lambda wildcards: (
            ""
            if SAMPLE2LIB[wildcards.sample] == "UNSTRANDED"
            else "--directional-mapping"
        ),
        splice_args=(
            "--pen-noncansplice 20 --min-intronlen 20 --max-intronlen 20"
            if SPLICE_GENOME
            else "--no-spliced-alignment"
        ),
        secondary_args=(
            f"--secondary-change {config['secondary_change']}"
            if config.get("secondary_change")
            else ""
        ),
    threads: 64
    benchmark:
        BENCHDIR / "remap_align_se_{sample}_{rn}.benchmark.txt"
    shell:
        """
        set -eo pipefail
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -U {input.fq} --base-change {params.basechange} {params.secondary_args} {params.directional} {params.splice_args} \
            --avoid-pseudogene --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-3,-0.5 |\
            {PATH.samtools} view -e '(([NS] + [NC]*0.2) / qlen) <= 0.08 && !flag.secondary' -@ {threads} -U {output.unmapped} --save-counts {output.report} -O BAM -o {output.bam}
        """


rule finalize_remap_summary:
    input:
        lambda wildcards: (
            TEMPDIR / f"remap/PE/{wildcards.sample}_{wildcards.rn}.summary"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"remap/SE/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/remap/{sample}_{rn}.summary",
    benchmark:
        BENCHDIR / "finalize_remap_summary_{sample}_{rn}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule finalize_genome_bam:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"remap/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.mapped.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.genome.bam",
    threads: 64
    benchmark:
        BENCHDIR / "finalize_genome_bam_{sample}_{rn}.benchmark.txt"
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule finalize_genome_report:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"remap/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.report_mqc.tsv"
        ),
    output:
        INTERNALDIR / "stats/filter/{sample}_{rn}.genome_mqc.tsv",
    benchmark:
        BENCHDIR / "finalize_genome_report_{sample}_{rn}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule remap_get_unmapped:
    input:
        un=TEMPDIR / "remap/{libmode}/{sample}_{rn}.final_unmap.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/remap/{libmode}/{sample}_{rn}_R1.fq.gz"),
        r2=temp(TEMPDIR / "unmapped/remap/{libmode}/{sample}_{rn}_R2.fq.gz"),
    benchmark:
        BENCHDIR / "remap_get_unmapped_{libmode}_{sample}_{rn}.benchmark.txt"
    shell:
        """
        if [ "{wildcards.libmode}" == "PE" ]; then
            {PATH.samtools} fastq -F 0x900 -1 {output.r1} -2 {output.r2} -0 /dev/null -s /dev/null -n {input}
        else
            touch {output.r2}
            {PATH.samtools} fastq -F 0x900 -0 {output.r1} -n {input}
        fi
        """


rule finalize_unmapped_fq:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"unmapped/remap/PE/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}.fq.gz"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR
            / f"unmapped/remap/SE/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}.fq.gz"
        ),
    output:
        INTERNALDIR / "fastq/unmapped/{sample}_{rn}_{rd}.fq.gz",
    benchmark:
        BENCHDIR / "finalize_unmapped_fq_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule unmapped_qc:
    input:
        INTERNALDIR / "fastq/unmapped/{sample}_{rn}_{rd}.fq.gz",
    output:
        html=INTERNALDIR / "qc/fastqc_unmapped/{sample}_{rn}_{rd}/fastqc_report.html",
        text=INTERNALDIR / "qc/fastqc_unmapped/{sample}_{rn}_{rd}/fastqc_data.txt",
        summary=INTERNALDIR / "qc/fastqc_unmapped/{sample}_{rn}_{rd}/summary.txt",
    params:
        lambda wildcards: INTERNALDIR
        / f"qc/fastqc_unmapped/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}",
    benchmark:
        BENCHDIR / "unmapped_qc_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        "{PATH.falco} -o {params} {input}"


rule unmapped_report:
    input:
        [
            INTERNALDIR / f"qc/fastqc_unmapped/{s}_{r}_{i}/fastqc_data.txt"
            for s, v in SAMPLE2DATA.items()
            for r, v2 in v.items()
            for i in ["R1", "R2"]
            if i in v2 or (i == "R2" and len(v2) == 2)
        ],
    output:
        "report_reads/unmapped.html",
    benchmark:
        BENCHDIR / "unmapped_report.benchmark.txt"
    shell:
        "{PATH.report_html} qc {output} {input}"


#######################
# combine runs
#######################


rule combine_bams:
    input:
        lambda wildcards: [
            INTERNALDIR / f"bam/per_run/{wildcards.sample}_{r}.{wildcards.reftype}.bam"
            for r in SAMPLE2DATA[wildcards.sample]
        ],
    output:
        bam=temp(TEMPDIR / "combined/{sample}.{reftype}.bam"),
        bai=temp(TEMPDIR / "combined/{sample}.{reftype}.bam.bai"),
    threads: 64
    benchmark:
        BENCHDIR / "combine_bams_{sample}_{reftype}.benchmark.txt"
    shell:
        """
        mkdir -p $(dirname {output.bam})
        {PATH.samtools} merge -@ {threads} -f --write-index -o {output.bam}##idx##{output.bai} {input}
        """


rule stat_combined:
    input:
        bam=TEMPDIR / "combined/{sample}.{reftype}.bam",
    output:
        stat=INTERNALDIR / "stats/combined/{sample}.{reftype}.txt",
        n=INTERNALDIR / "stats/combined/{sample}.{reftype}.count",
    threads: 4
    benchmark:
        BENCHDIR / "stat_combined_{sample}_{reftype}.benchmark.txt"
    shell:
        """
        {PATH.samtools} flagstat -@ {threads} -O TSV {input} > {output.stat}
        {PATH.samtools} view -@ {threads} -c -F 384 {input} > {output.n}
        """


rule drop_duplicates:
    input:
        bam=TEMPDIR / "combined/{sample}.{reftype}.bam",
        bai=TEMPDIR / "combined/{sample}.{reftype}.bam.bai",
    output:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
        txt=INTERNALDIR / "stats/dedup/{sample}.{reftype}.log",
    threads: 64
    benchmark:
        BENCHDIR / "drop_duplicates_{sample}_{reftype}.benchmark.txt"
    shell:
        "{PATH.markdup} -t {threads} -i {input.bam} -o {output.bam} --report {output.txt}"


rule dedup_index:
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        bai=INTERNALDIR / "bam/{sample}.{reftype}.bam.bai",
    threads: 8
    benchmark:
        BENCHDIR / "dedup_index_{sample}_{reftype}.benchmark.txt"
    shell:
        "{PATH.samtools} index -@ {threads} {input}"


rule stat_dedup:
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        stat=INTERNALDIR / "stats/dedup/{sample}.{reftype}.txt",
        n=INTERNALDIR / "stats/dedup/{sample}.{reftype}.count",
    threads: 4
    benchmark:
        BENCHDIR / "stat_dedup_{sample}_{reftype}.benchmark.txt"
    shell:
        """
        {PATH.samtools} flagstat -@ {threads} -O TSV {input} > {output.stat}
        {PATH.samtools} view -@ {threads} -c -F 384 {input} > {output.n}
        """


rule liftover_transcript_to_genome:
    input:
        transcripts=INTERNALDIR / "bam/{sample}.transcript.bam",
        genome=INTERNALDIR / "bam/{sample}.genome.bam",
        info=INTERNALDIR / "ref/transcript.tsv",
    output:
        transcripts=temp(TEMPDIR / "liftover/{sample}.transcript.bam"),
        bam=INTERNALDIR / "liftover_bam/{sample}.bam",
    params:
        fai=REF["genome"]["fa"] + ".fai",
    threads: 8
    benchmark:
        BENCHDIR / "liftover_transcript_to_genome_{sample}.benchmark.txt"
    shell:
        """
        {PATH.coralsnake} liftover -t {threads} -i {input.transcripts} -o {output.transcripts} -a {input.info} -f {params.fai}
        {PATH.samtools} cat {output.transcripts} {input.genome} | {PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output.bam}
        """


rule count_reads:
    input:
        report=lambda wildcards: [
            INTERNALDIR / f"qc/trimming/{wildcards.sample}_{r}_mqc.tsv"
            for r in SAMPLE2DATA[wildcards.sample].keys()
        ],
        count1=(
            INTERNALDIR / "stats/combined/{sample}.contamination.count"
            if HAS_CONTAM
            else []
        ),
        count2=(
            INTERNALDIR / "stats/dedup/{sample}.contamination.count"
            if HAS_CONTAM
            else []
        ),
        count3=(
            INTERNALDIR / "stats/combined/{sample}.genes.count" if HAS_GENES else []
        ),
        count4=(INTERNALDIR / "stats/dedup/{sample}.genes.count" if HAS_GENES else []),
        count5=INTERNALDIR / "stats/combined/{sample}.transcript.count",
        count6=INTERNALDIR / "stats/dedup/{sample}.transcript.count",
        count7=INTERNALDIR / "stats/combined/{sample}.genome.count",
        count8=INTERNALDIR / "stats/dedup/{sample}.genome.count",
    output:
        INTERNALDIR / "stats/count/{sample}.tsv",
    threads: 2
    benchmark:
        BENCHDIR / "count_reads_{sample}.benchmark.txt"
    shell:
        """
        printf "Raw\\t"$(grep -h -P 'input": [0-9]+,' -m 1 {input.report} |awk '{{ gsub(",","",$NF);a+=$NF }}END{{ print a }}')"\\n" > {output}
        printf "Clean\\t"$(grep -h -P 'output": [0-9]+,' -m 1 {input.report} |awk '{{ gsub(",","",$NF);a+=$NF }}END{{ print a }}')"\\n" >> {output}
        if [ -n '{input.count1}' ] && [ -s '{input.count1}' ]; then
            printf "Contamination_Passed\\t"$(cat {input.count1})"\\n" >> {output}
        fi
        if [ -n '{input.count2}' ] && [ -s '{input.count2}' ]; then
            printf "Contamination_Dedup\\t"$(cat {input.count2})"\\n" >> {output}
        fi
        if [ -n '{input.count3}' ] && [ -s '{input.count3}' ]; then
            printf "Masking_Passed\\t"$(cat {input.count3})"\\n" >> {output}
        fi
        if [ -n '{input.count4}' ] && [ -s '{input.count4}' ]; then
            printf "Masking_Dedup\\t"$(cat {input.count4})"\\n" >> {output}
        fi
        printf "Transcript_Passed\\t"$(cat {input.count5})"\\n" >> {output}
        printf "Transcript_Dedup\\t"$(cat {input.count6})"\\n" >> {output}
        printf "Genome_Passed\\t"$(cat {input.count7})"\\n" >> {output}
        printf "Genome_Dedup\\t"$(cat {input.count8})"\\n" >> {output}
        """


rule insert_size:
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        tsv=INTERNALDIR / "stats/rlen/{sample}.{reftype}.isize.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "insert_size_{sample}_{reftype}.benchmark.txt"
    shell:
        """
        {PATH.samtools} stats -@ {threads} -i 1000 {input} |grep ^IS|cut -f 2- > {output}
        """


rule read_length:
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        tsv=INTERNALDIR / "stats/rlen/{sample}.{reftype}.rlen.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "read_length_{sample}_{reftype}.benchmark.txt"
    shell:
        """
        {PATH.samtools} stats -@ {threads} -i 1000 {input} |grep ^RL | cut -f 2- > {output}
        """


###################
# call sites
###################


rule cal_spike_ratio:
    input:
        bam=lambda wildcards: (
            expand(INTERNALDIR / "bam/{sample}.genes.bam", sample=SAMPLE2DATA.keys())
            if HAS_GENES
            else []
        ),
        bai=lambda wildcards: (
            expand(
                INTERNALDIR / "bam/{sample}.genes.bam.bai",
                sample=SAMPLE2DATA.keys(),
            )
            if HAS_GENES
            else []
        ),
    output:
        tsv=INTERNALDIR / "stats/ratio/probe.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "cal_spike_ratio.benchmark.txt"
    shell:
        """
        {PATH.bam_conv} {input.bam} > {output}
        """


rule run_countmut:
    """countmut >= 0.2.2: native per-strand 2-group conversion view
    (chrom pos strand motif u0 u1 m0 m1), written straight by countmut --
    the pileup_reformat.py bridge is gone.

    u0/u1 = reference-base counts (default A; C when pileup_ct), m0/m1 =
    mutation-base counts (default G; T when pileup_ct).  The single -e
    expression is a group router re-expressing the legacy 0.0.8 read gate
    (the pipeline's historical defaults: NS<=1, Yf>=1, Zf<=3, baseq>=20,
    2bp/2bp read-end trim; mapq>=0 is a no-op):

      group 1 = bases passing the high-conversion gate,
      group 0 = all other kept bases (low quality / read-end positions),
      NS > max_sub drops the read entirely (nil) -- exactly as 0.0.8
      discarded failing reads.

    Group 1 (u1/m1) is byte-for-byte the legacy gated count set, so the
    downstream consumers (unfilter_genes_stat / motif_conversion_rate_stat /
    merge_samples) keep computing on u1/m1 only; u0/m0 are extra columns in
    the TSV.  NOTE: the 0.0.8 conversion gate reads the Yf/Zf
    (forward-channel) tags even for the C->T view -- kept here for parity.
    The 31-mer motif (pad 15) must stay in sync with substr($4,15,3) in
    motif_conversion_rate_stat.
    """
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
        bai=INTERNALDIR / "bam/{sample}.{reftype}.bam.bai",
        ref=lambda wildcards: (
            INTERNALDIR / "ref/transcript.fa"
            if wildcards.reftype == "transcript"
            else (
                INTERNALDIR / "ref/genes.fa"
                if wildcards.reftype == "genes"
                else REF["genome"]["fa"]
            )
        ),
    output:
        temp(TEMPDIR / "pileup/{sample}.{reftype}.tsv"),
    params:
        # 2-group router (countmut >= 0.2.2): group 1 = high-conversion bases
        # (legacy 0.0.8 gate), group 0 = all other kept bases; NS > max_sub
        # drops the read (nil).  With equal trims the legacy per-strand
        # condition reduces to `qpos >= trim and qlen - qpos > trim`.
        router=lambda wildcards: (
            "([NS] <= {}) and (([Yf] >= {} and [Zf] <= {} and bq >= {}"
            " and qpos >= {} and qlen - qpos > {}) and 1 or 0)"
        ).format(
            config.get("countmut_max_sub", 1),
            config.get("countmut_min_con", 1),
            config.get("countmut_max_unc", 3),
            config.get("countmut_min_baseq", 20),
            config.get("countmut_trim", 2),
            config.get("countmut_trim", 2),
        ),
        # Target-base sites only (like the old bridge's row filter
        # `ref == base and u+m > 0`).  -p sees both-strand all-group totals,
        # so a strand row with only group-0 counts may still appear; all
        # downstream consumers guard on u1+m1 > 0.
        site_filter=lambda wildcards: (
            "ref == 'C' and (c + t) > 0"
            if config.get("pileup_ct", False)
            else "ref == 'A' and (a + g) > 0"
        ),
        # \t is expanded by the C core (\t in --fmt-header; Lua string
        # literal in --output-format), so the shell sees plain text.
        fmt_header="chrom\\tpos\\tstrand\\tmotif\\tu0\\tu1\\tm0\\tm1",
        output_fmt=lambda wildcards: (
            "{chrom}\\t{pos+1}\\t{strand}\\t{motif}\\t{c.0}\\t{c.1}\\t{t.0}\\t{t.1}"
            if config.get("pileup_ct", False)
            else "{chrom}\\t{pos+1}\\t{strand}\\t{motif}\\t{a.0}\\t{a.1}\\t{g.0}\\t{g.1}"
        ),
    threads: 64
    benchmark:
        BENCHDIR / "run_countmut_{sample}_{reftype}.benchmark.txt"
    shell:
        "{PATH.countmut} -i {input.bam} -r {input.ref} -o {output} -t {threads} -e \"{params.router}\" -p \"{params.site_filter}\" --motif-pad 15 --fmt-header \"{params.fmt_header}\" --output-format \"{params.output_fmt}\" > /dev/null"


rule pileup_base:
    input:
        TEMPDIR / "pileup/{sample}.{reftype}.tsv",
    output:
        INTERNALDIR / "pileup/per_sample/{sample}.{reftype}.tsv.gz",
    threads: 64
    benchmark:
        BENCHDIR / "pileup_base_{sample}_{reftype}.benchmark.txt"
    shell:
        "{PATH.bgzip} -@ {threads} -c {input} > {output}"


rule unfilter_genes_stat:
    """Per-reference summary of the 8-column pileup (chrom pos strand motif
    u0 u1 m0 m1): unconverted = u1 (group 1 = the legacy gated set),
    depth = u1+m1, ratio = u1/(u1+m1) -- byte-for-byte the pre-router
    numbers (group 0 is not used here; motif_conversion_rate_stat reports it
    in the *_all columns).  NOTE: the legacy awk keyed `u` on $6 (the u1
    value) while d/r/n were keyed on $1, so the END block divided by zero on
    any non-empty input (fatal in gawk/mawk); `u[$1]` is the evident intent
    (per-reference summary) and is what this rule now does."""
    input:
        INTERNALDIR / "pileup/per_sample/{sample}.{reftype}.tsv.gz",
    output:
        INTERNALDIR / "stats/{sample}.{reftype}.genes.tsv",
    benchmark:
        BENCHDIR / "unfilter_genes_stat_{sample}_{reftype}.benchmark.txt"
    shell:
        """
        zcat {input} | awk -F '\\t' 'NR>1 && $1!~"^probe_" && ($6+$8+0)>0{{u[$1]+=$6; d[$1]+=$6+$8; r[$1]+=$6/($6+$8); n[$1]+=1}}END{{ for(x in u){{print x,n[x],u[x],d[x],r[x]/n[x]}} }}' > {output}
        """


rule motif_conversion_rate_stat:
    """Per-3-mer conversion rate around target-base sites from the 8-column
    pileup (chrom pos strand motif u0 u1 m0 m1; motif 31-mer, center = 16th
    base, so the 3-mer is substr($4,15,3)).  The first five columns keep the
    legacy schema AND values (group 1 = the legacy gated set):
    Motif/Count/Unconverted/Depth/Ratio.  The three *_all columns additionally
    report the same per-motif stats over ALL kept groups (u0+u1 unconverted,
    u0+u1+m0+m1 depth)."""
    input:
        pileup=INTERNALDIR / "pileup/per_sample/{sample}.{reftype}.tsv.gz",
    output:
        INTERNALDIR / "stats/ratio/by_motif/{sample}.{reftype}.tsv",
    params:
        target_base=config.get("base_change", "A,G").split(",")[0].upper(),
    benchmark:
        BENCHDIR / "motif_conversion_rate_stat_{sample}_{reftype}.benchmark.txt"
    shell:
        "zcat {input.pileup} | awk -F '\\t' -v target=\"{params.target_base}\" "
        '\'BEGIN{{OFS="\\t";print "Motif","Count","Unconverted","Depth","Ratio","Count_all","Unconverted_all","Depth_all","Ratio_all"}} '
        "NR>1 && ($5+$6+$7+$8+0)>0{{ "
        "m=toupper(substr($4,15,3)); "
        'if(m ~ "^[ATGC]+$" && substr(m,2,1) == target){{ '
        "a=$5+$6;g=$7+$8;d=a+g;na[m]++;ua[m]+=a;da[m]+=d;ra[m]+=a/d;"
        "if(($6+$8+0)>0){{n1[m]++;u1[m]+=$6;d1[m]+=$6+$8;r1[m]+=$6/($6+$8)}}}}"
        "END{{for(m in da) print m,(n1[m]+0),(u1[m]+0),(d1[m]+0),(n1[m]>0?r1[m]/n1[m]:0),na[m],ua[m],da[m],ra[m]/na[m]}}' > {output}"


rule join_pileup_table:
    input:
        expand(
            INTERNALDIR / "pileup/per_sample/{sample}.{{reftype}}.tsv.gz",
            sample=SAMPLE2DATA.keys(),
        ),
    output:
        INTERNALDIR / "pileup/{reftype}.parquet",
    params:
        samples=" ".join(SAMPLE2DATA.keys()),
        requires=" ".join(
            [("0" if s in SKIP_SAMPLES else "1") for s in SAMPLE2DATA.keys()]
        ),
    threads: lambda wildcards, input: min(int(len(input) * 4), 32)
    benchmark:
        BENCHDIR / "join_pileup_table_{reftype}.benchmark.txt"
    shell:
        """
        {PATH.merge_samples} --files {input} --names {params.samples} --output {output} --requires {params.requires}
        """


rule merge_gene_and_genome_table:
    input:
        info=INTERNALDIR / "ref/transcript.tsv",
        transcripts=INTERNALDIR / "pileup/transcript.parquet",
        genome=INTERNALDIR / "pileup/genome.parquet",
    output:
        "report_sites/sites.tsv.gz",
    threads: 32
    benchmark:
        BENCHDIR / "merge_gene_and_genome_table.benchmark.txt"
    resources:
        runtime=720
    shell:
        """
        {PATH.remap_genome} -t {input.info} -a {input.transcripts} -b {input.genome} -o {output} --min-depth {config[min_merged_depth]}
        """


rule filter_eTAM_sites:
    input:
        "report_sites/sites.tsv.gz",
    output:
        fl="report_sites/filtered.tsv",
    threads: 64
    benchmark:
        BENCHDIR / "filter_eTAM_sites.benchmark.txt"
    shell:
        """
        {PATH.filter_sites} -i {input} -o {output.fl}
        """


rule group_and_pval_cal:
    input:
        "report_sites/sites.tsv.gz",
    output:
        "report_sites/grouped/{group}.parquet",
    params:
        names=lambda wildcards: GROUP2SAMPLE[wildcards.group],
    threads: 8
    benchmark:
        BENCHDIR / "group_and_pval_cal_{group}.benchmark.txt"
    shell:
        """
        {PATH.sum_groups} -i {input} -o {output} -n {params.names}
        """


# multiqc custom


rule mqc_aggregate_mapping_stats:
    input:
        counts=expand(
            INTERNALDIR / "stats/count/{sample}.tsv", sample=SAMPLE2DATA.keys()
        ),
        dedup_logs=expand(
            INTERNALDIR / "stats/dedup/{sample}.{reftype}.log",
            sample=SAMPLE2DATA.keys(),
            reftype=[
                r
                for r in ["genome", "transcript", "genes", "contamination"]
                if (r != "genes" or HAS_GENES)
                and (r != "contamination" or HAS_CONTAM)
            ],
        ),
        trim_jsons=expand(
            INTERNALDIR / "qc/trimming/{sample}_{rn}_mqc.tsv",
            sample=SAMPLE2DATA.keys(),
            rn=["run1"],
        ),
    output:
        mapping=INTERNALDIR / "stats/mqc/reads/mapping_stats_mqc.tsv",
        dedup=INTERNALDIR / "stats/mqc/reads/dedup_stats_mqc.tsv",
    benchmark:
        BENCHDIR / "mqc_aggregate_mapping_stats.benchmark.txt"
    threads: 4
    shell:
        """
        {PATH.mqc_mapping} {output.mapping} {output.dedup} {input.counts} --dedup-logs {input.dedup_logs} --trim-jsons {input.trim_jsons}
        """


rule mqc_aggregate_site_stats:
    input:
        motifs=expand(
            INTERNALDIR / "stats/ratio/by_motif/{sample}.{reftype}.tsv",
            sample=SAMPLE2DATA.keys(),
            reftype=["transcript", "genome"],
        ),
        sites_file=[
            INTERNALDIR / "pileup/genome.parquet",
            INTERNALDIR / "pileup/transcript.parquet",
        ],
    output:
        motifs=INTERNALDIR / "stats/mqc/sites/motif_conversion_mqc.tsv",
        site_sum=INTERNALDIR / "stats/mqc/sites/site_summary_mqc.tsv",
        site_dist=INTERNALDIR / "stats/mqc/sites/site_distribution_mqc.tsv",
        site_depth=INTERNALDIR / "stats/mqc/sites/site_depth_mqc.tsv",
        motif_transcript=INTERNALDIR / "stats/mqc/sites/motif_ratio_transcript_mqc.tsv",
        motif_genome=INTERNALDIR / "stats/mqc/sites/motif_ratio_genome_mqc.tsv",
    params:
        target_base=config.get("base_change", "A,G").split(",")[0],
    benchmark:
        BENCHDIR / "mqc_aggregate_site_stats.benchmark.txt"
    threads: 16
    resources:
        runtime=720
    shell:
        """
        {PATH.mqc_sites} {output.motifs} {output.site_sum} {output.site_dist} {output.site_depth} {output.motif_transcript} {output.motif_genome} --motif-files {input.motifs} --sites-file {input.sites_file} --target-base {params.target_base}
        """


rule generate_mapping_report:
    input:
        INTERNALDIR / "stats/mqc/reads/mapping_stats_mqc.tsv",
        INTERNALDIR / "stats/mqc/reads/dedup_stats_mqc.tsv",
        expand(
            INTERNALDIR / "stats/premap/{sample}_{rn}.summary",
            sample=SAMPLE2DATA.keys(),
            rn=["run1"],
        )
        if HAS_CONTAM
        else [],
        expand(
            INTERNALDIR / "stats/mainmap/{sample}_{rn}.summary",
            sample=SAMPLE2DATA.keys(),
            rn=["run1"],
        ),
        expand(
            INTERNALDIR / "stats/remap/{sample}_{rn}.summary",
            sample=SAMPLE2DATA.keys(),
            rn=["run1"],
        ),
    output:
        "report_reads/mapping.html",
    params:
        report_name="mapping.html",
        report_dir=str(Path("report_reads")),
    benchmark:
        BENCHDIR / "generate_mapping_report.benchmark.txt"
    shell:
        "{PATH.report_html} tables {output} {input}"


rule generate_site_report:
    input:
        INTERNALDIR / "stats/mqc/sites/motif_conversion_mqc.tsv",
        INTERNALDIR / "stats/mqc/sites/site_summary_mqc.tsv",
        INTERNALDIR / "stats/mqc/sites/site_distribution_mqc.tsv",
        INTERNALDIR / "stats/mqc/sites/site_depth_mqc.tsv",
        INTERNALDIR / "stats/mqc/sites/motif_ratio_transcript_mqc.tsv",
        INTERNALDIR / "stats/mqc/sites/motif_ratio_genome_mqc.tsv",
    output:
        "report_sites/sites.html",
    params:
        report_name="sites.html",
        report_dir=str(Path("report_sites")),
    benchmark:
        BENCHDIR / "generate_site_report.benchmark.txt"
    shell:
        "{PATH.report_html} tables {output} {input}"


rule generate_metagene_profile:
    """Metagene coverage distribution of the remapped sites (machine-readable)."""
    input:
        sites="report_sites/sites.tsv.gz",
        gtf=REF["genome"]["gtf"],
    output:
        prof=INTERNALDIR / "stats/report/metagene_profile.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "generate_metagene_profile.benchmark.txt"
    shell:
        """
        {PATH.coralsnake} metagene -i {input.sites} -g {input.gtf} -H \
            --meta-columns 1,2,3 --bins 100 --export-profile {output.prof}
        """


rule generate_logo_matrix:
    """Sequence-context logo around remapped sites (matrix, not a figure).

    Uses the per-site context already present in the sites table (`Motif`
    column), weighted by total depth across libraries.
    """
    input:
        sites="report_sites/sites.tsv.gz",
    output:
        matrix=INTERNALDIR / "stats/report/logo_matrix.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "generate_logo_matrix.benchmark.txt"
    shell:
        """
        zcat {input.sites} \
          | awk -F '\\t' 'NR==1{{for(i=7;i<=NF;i++) if($$i ~ /^Depth_/) d[i]=1; next}} \
              {{s=0; for(i in d) s+=$$i; if($$6 ~ /^[ACGTUNn]+$$/ && s>0) print $$6 "\\t" s}}' \
          | {PATH.coralsnake} logo -i - --matrix {output.matrix}
        """


rule generate_sites_extra_report:
    """Render metagene coverage + sequence logo sections (sample-independent)."""
    input:
        prof=rules.generate_metagene_profile.output.prof,
        matrix=rules.generate_logo_matrix.output.matrix,
    output:
        meta=INTERNALDIR / "stats/report/metagene.html",
        logo=INTERNALDIR / "stats/report/logo.html",
    benchmark:
        BENCHDIR / "generate_sites_extra_report.benchmark.txt"
    shell:
        """
        {PATH.report_html} metagene {output.meta} {input.prof}
        {PATH.report_html} logo {output.logo} {input.matrix}
        """


rule generate_motifconv_report:
    """Per-motif conversion-rate section (one per sample x reftype)."""
    input:
        INTERNALDIR / "stats/ratio/by_motif/{sample}.{reftype}.tsv",
    output:
        INTERNALDIR / "stats/report/motif.{sample}.{reftype}.html",
    benchmark:
        BENCHDIR / "generate_motifconv_report_{sample}_{reftype}.benchmark.txt"
    shell:
        "{PATH.report_html} motifconv {output} {input}"


rule final_report:
    """Assemble one self-contained report.html from all per-section HTML."""
    input:
        "report_reads/trimmed.html",
        "report_reads/unmapped.html",
        "report_reads/mapping.html",
        "report_sites/sites.html",
        rules.generate_sites_extra_report.output,
        expand(
            rules.generate_motifconv_report.output,
            sample=SAMPLE2DATA.keys(),
            reftype=["transcript", "genome"],
        ),
    output:
        "report.html",
    benchmark:
        BENCHDIR / "final_report.benchmark.txt"
    shell:
        """
        mkdir -p report_reads report_sites
        {PATH.report_html} assemble {output} {input}
        """
