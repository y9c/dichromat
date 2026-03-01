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



REF = config.get("reference", {})
# Expand user paths and resolve relative paths in REF dictionary
for ref_type in REF:
    if isinstance(REF[ref_type], dict):
        for key, val in REF[ref_type].items():
            if isinstance(val, str):
                # Expand user (~) and resolve relative paths
                val = os.path.expanduser(val)
                if not os.path.isabs(val):
                    val = os.path.normpath(os.path.join(workflow.basedir, val))
                REF[ref_type][key] = val
    elif isinstance(REF[ref_type], list):
        resolved_list = []
        for f in REF[ref_type]:
            if isinstance(f, str):
                f = os.path.expanduser(f)
                if not os.path.isabs(f):
                    f = os.path.normpath(os.path.join(workflow.basedir, f))
            resolved_list.append(f)
        REF[ref_type] = resolved_list
    elif isinstance(REF[ref_type], str):
        val = os.path.expanduser(REF[ref_type])
        if not os.path.isabs(val):
            val = os.path.normpath(os.path.join(workflow.basedir, val))
        REF[ref_type] = val


TEMPDIR = Path(
    os.path.relpath(
        config.get("tempdir", os.path.join(workflow.basedir, ".tmp")), workflow.basedir
    )
)
# Convert PATH dict to SimpleNamespace for dot notation access (e.g., PATH.python instead of PATH['python'])
PATH = SimpleNamespace(**config.get("path", {}))

INTERNALDIR = Path("internal_files")
BENCHDIR = Path(".snakemake/benchmarks")
BENCHDIR = Path(".snakemake/benchmarks")
MARKDUP = config.get("markdup", True)
SPLICE_GENOME = config.get("splice_genome", True)
SPLICE_CONTAM = config.get("splice_contamination", False)


wildcard_constraints:
    sample=r"[^/_\.]+",
    rn=r"run[0-9]+",


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
    SAMPLE2LIB[s] = v.get("libtype", config.get("libtype", "")) # Built-in Name
    SAMPLE2ADP[s] = v.get("adapter", config.get("adapter", "")) # Custom Sequence
    if "group" in v:
        GROUP2SAMPLE[v["group"]].append(s)
    for i, v2 in enumerate(v["data"], 1):
        r = f"run{i}"
        SAMPLE2DATA[str(s)][r] = {
            k: os.path.expanduser(v3) for k, v3 in dict(v2).items()
        }

HAS_GENES = bool(config.get("genes"))
HAS_CONTAM = bool(config.get("contamination"))

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
    benchmark:
        BENCHDIR / "all.benchmark.txt"
    benchmark:
        BENCHDIR / "all.benchmark.txt"
    input:
        "report_reads/mapping.html",
        "report_reads/trimmed.html",
        "report_reads/unmapped.html",
        "report_sites/sites.html",
        "report_sites/filtered.tsv" if IS_ETAM else "report_sites/merged.tsv.gz",
        expand("report_sites/grouped/{group}.parquet", group=GROUP2SAMPLE.keys()),


# prepare ref


rule combine_contamination_fa:
    benchmark:
        BENCHDIR / "combine_contamination_fa.benchmark.txt"
    benchmark:
        BENCHDIR / "combine_contamination_fa.benchmark.txt"
    input:
        REF.get("contamination", []) if "contamination" in REF else [],
    output:
        fa=INTERNALDIR / "ref/contamination.fa",
        fai=INTERNALDIR / "ref/contamination.fa.fai",
    shell:
        """
        mkdir -p $(dirname {output.fa})
        cat {input} > {output.fa}
        {PATH.samtools} faidx {output.fa} --fai-idx {output.fai}
        """


rule build_contamination_hisat3n_index:
    benchmark:
        BENCHDIR / "build_contamination_hisat3n_index.benchmark.txt"
    benchmark:
        BENCHDIR / "build_contamination_hisat3n_index.benchmark.txt"
    input:
        INTERNALDIR / "ref/contamination.fa",
    output:
        INTERNALDIR / "ref/contamination.3n.GA.1.ht2",
    params:
        basechange=config.get("base_change", "A,G"),
        prefix=str(INTERNALDIR / "ref/contamination"),
    threads: 8
    shell:
        """
        rm -f {params.prefix}*.ht2
        {PATH.hisat3nbuild} -p {threads} --base-change {params.basechange} {input} {params.prefix}
        """


rule combine_genes_fa:
    benchmark:
        BENCHDIR / "combine_genes_fa.benchmark.txt"
    benchmark:
        BENCHDIR / "combine_genes_fa.benchmark.txt"
    input:
        REF.get("genes", []) if "genes" in REF else [],
    output:
        fa=INTERNALDIR / "ref/genes.fa",
        fai=INTERNALDIR / "ref/genes.fa.fai",
    shell:
        """
        mkdir -p $(dirname {output.fa})
        cat {input} > {output.fa}
        {PATH.samtools} faidx {output.fa} --fai-idx {output.fai}
        """


rule prepared_transcript_ref:
    benchmark:
        BENCHDIR / "prepared_transcript_ref.benchmark.txt"
    benchmark:
        BENCHDIR / "prepared_transcript_ref.benchmark.txt"
    input:
        fa=REF["genome"]["fa"],
        gtf=REF["genome"]["gtf"],
    output:
        info=INTERNALDIR / "ref/transcript.tsv",
        seq=INTERNALDIR / "ref/transcript.fa",
    shell:
        """
        mkdir -p $(dirname {output.info})
        coralsnake prepare -g {input.gtf} -f {input.fa} -o {output.info} -s {output.seq} -c -n -x -t -z
        """


# cut adapters


rule trim_se:
    benchmark:
        BENCHDIR / "trim_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "trim_se.benchmark.txt"
    input:
        lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R1") or [],
    output:
        c=temp(TEMPDIR / "trim/SE/{sample}_{rn}_R1.fq.gz"),
        s=temp(TEMPDIR / "trim/SE/{sample}_{rn}_tooshort_R1.fq.gz"),
        report=temp(TEMPDIR / "trim/SE/{sample}_{rn}.json"),
    params:
        minlen=config.get("min_len", 20),
        cut=lambda wildcards: (
            f"-A '{SAMPLE2LIB[wildcards.sample]}'"
            if SAMPLE2LIB[wildcards.sample]
            else f"-a '{SAMPLE2ADP[wildcards.sample]}'"
        ),
    threads: 24
    shell:
        """
        {PATH.cutseq} -t {threads} {params.cut} -m {params.minlen} --auto-rc -o {output.c} -s {output.s} --json-file {output.report} {input} 
        """


rule trim_pe:
    benchmark:
        BENCHDIR / "trim_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "trim_pe.benchmark.txt"
    input:
        r1=lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R1") or [],
        r2=lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R2") or [],
    output:
        c1=temp(TEMPDIR / "trim/PE/{sample}_{rn}_R1.fq.gz"),
        c2=temp(TEMPDIR / "trim/PE/{sample}_{rn}_R2.fq.gz"),
        s1=temp(TEMPDIR / "trim/PE/{sample}_{rn}_tooshort_R1.fq.gz"),
        s2=temp(TEMPDIR / "trim/PE/{sample}_{rn}_tooshort_R2.fq.gz"),
        report=temp(TEMPDIR / "trim/PE/{sample}_{rn}.json"),
    params:
        minlen=config.get("min_len", 20),
        cut=lambda wildcards: (
            f"-A '{SAMPLE2LIB[wildcards.sample]}'"
            if SAMPLE2LIB[wildcards.sample]
            else f"-a '{SAMPLE2ADP[wildcards.sample]}'"
        ),
    threads: 24
    shell:
        """
        {PATH.cutseq} -t {threads} {params.cut} -m {params.minlen} --auto-rc -o {output.c1} -p {output.c2} -s {output.s1} -S {output.s2} --json-file {output.report} {input.r1} {input.r2}
        """


rule finalize_trim_report:
    benchmark:
        BENCHDIR / "finalize_trim_report_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_trim_report.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"trim/PE/{wildcards.sample}_{wildcards.rn}.json"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"trim/SE/{wildcards.sample}_{wildcards.rn}.json"
        ),
    output:
        INTERNALDIR / "qc/trimming/{sample}_{rn}.json",
    shell:
        "cp {input} {output}"


rule finalize_discarded_reads:
    benchmark:
        BENCHDIR / "finalize_discarded_reads_{sample}_{rn}_{rd}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_discarded_reads.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR
            / f"trim/PE/{wildcards.sample}_{wildcards.rn}_tooshort_{wildcards.rd}.fq.gz"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR
            / f"trim/SE/{wildcards.sample}_{wildcards.rn}_tooshort_{wildcards.rd}.fq.gz"
        ),
    output:
        INTERNALDIR / "qc/{sample}_{rn}_tooshort_{rd}.fq.gz",
    shell:
        "cp {input} {output}"


# trimmed part qc


rule qc_trimmed:
    benchmark:
        BENCHDIR / "qc_trimmed_{sample}_{rn}_{rd}.benchmark.txt"
    benchmark:
        BENCHDIR / "qc_trimmed.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR
            / f"trim/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}.fq.gz"
        ),
    output:
        html=INTERNALDIR / "qc/fastqc/{sample}_{rn}_{rd}/fastqc_report.html",
        text=INTERNALDIR / "qc/fastqc/{sample}_{rn}_{rd}/fastqc_data.txt",
        summary=INTERNALDIR / "qc/fastqc/{sample}_{rn}_{rd}/summary.txt",
    params:
        lambda wildcards: INTERNALDIR
        / f"qc/fastqc/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}",
    shell:
        "{PATH.falco} -o {params} {input}"


rule report_qc_trimmed:
    benchmark:
        BENCHDIR / "report_qc_trimmed.benchmark.txt"
    benchmark:
        BENCHDIR / "report_qc_trimmed.benchmark.txt"
    input:
        [
            INTERNALDIR / f"qc/fastqc/{sample}_{rn}_{rd}/fastqc_data.txt"
            for sample, v in SAMPLE2DATA.items()
            for rn, v2 in v.items()
            for rd in v2.keys()
        ],
    output:
        "report_reads/trimmed.html",
    shell:
        "{PATH.multiqc} -f -m fastqc -n {output} {input}"


# premap to contamination


rule premap_align_pe:
    benchmark:
        BENCHDIR / "premap_align_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "premap_align_pe.benchmark.txt"
    input:
        fq1=TEMPDIR / "trim/PE/{sample}_{rn}_R1.fq.gz",
        fq2=TEMPDIR / "trim/PE/{sample}_{rn}_R2.fq.gz",
        idx=INTERNALDIR / "ref/contamination.3n.GA.1.ht2",
    output:
        mapped=temp(TEMPDIR / "premap/PE/{sample}_{rn}.contam.bam"),
        unmapped=temp(TEMPDIR / "premap/PE/{sample}_{rn}.unmap.bam"),
        summary=temp(TEMPDIR / "premap/PE/{sample}_{rn}.summary"),
    params:
        index=str(INTERNALDIR / "ref/contamination"),
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
    threads: 36
    shell:
        """
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -1 {input.fq1} -2 {input.fq2} --base-change {params.basechange} {params.directional} {params.splice_args} \
            --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-2,-0.8 |\
            {PATH.samtools} view -@ {threads} -e 'flag.proper_pair && !flag.unmap && !flag.munmap && qlen-sclen >= 30 && [XM] * 15 < (qlen-sclen)' -O BAM -U {output.unmapped} -o {output.mapped}
        """


rule premap_align_se:
    benchmark:
        BENCHDIR / "premap_align_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "premap_align_se.benchmark.txt"
    input:
        fq=TEMPDIR / "trim/SE/{sample}_{rn}_R1.fq.gz",
        idx=INTERNALDIR / "ref/contamination.3n.GA.1.ht2",
    output:
        mapped=temp(TEMPDIR / "premap/SE/{sample}_{rn}.contam.bam"),
        unmapped=temp(TEMPDIR / "premap/SE/{sample}_{rn}.unmap.bam"),
        summary=temp(TEMPDIR / "premap/SE/{sample}_{rn}.summary"),
    params:
        index=str(INTERNALDIR / "ref/contamination"),
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
    threads: 36
    shell:
        """
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -U {input.fq} --base-change {params.basechange} {params.directional} {params.splice_args} \
            --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-2,-0.8 |\
            {PATH.samtools} view -@ {threads} -e '!flag.unmap && qlen-sclen >= 30 && [XM] * 15 < qlen-sclen' -O BAM -U {output.unmapped} -o {output.mapped}
        """


rule finalize_premap_summary:
    benchmark:
        BENCHDIR / "finalize_premap_summary_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_premap_summary.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"premap/PE/{wildcards.sample}_{wildcards.rn}.summary"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"premap/SE/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/premap/{sample}_{rn}.summary",
    shell:
        "cp {input} {output}"


rule premap_fixmate_pe:
    benchmark:
        BENCHDIR / "premap_fixmate_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "premap_fixmate_pe.benchmark.txt"
    input:
        TEMPDIR / "premap/PE/{sample}_{rn}.contam.bam",
    output:
        temp(TEMPDIR / "premap/PE/{sample}_{rn}.fixmate.bam"),
    threads: 8
    shell:
        "{PATH.samtools} fixmate -@ {threads} -m -O BAM {input} {output}"


rule premap_fixmate_se:
    benchmark:
        BENCHDIR / "premap_fixmate_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "premap_fixmate_se.benchmark.txt"
    input:
        TEMPDIR / "premap/SE/{sample}_{rn}.contam.bam",
    output:
        temp(TEMPDIR / "premap/SE/{sample}_{rn}.fixmate.bam"),
    threads: 8
    shell:
        "cp {input} {output}"


rule finalize_premap_bam:
    benchmark:
        BENCHDIR / "finalize_premap_bam_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_premap_bam.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"premap/PE/{wildcards.sample}_{wildcards.rn}.fixmate.bam"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"premap/SE/{wildcards.sample}_{wildcards.rn}.fixmate.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.contamination.bam",
    threads: 36
    priority: 4
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule premap_get_unmapped_pe:
    benchmark:
        BENCHDIR / "premap_get_unmapped_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "premap_get_unmapped_pe.benchmark.txt"
    input:
        un=TEMPDIR / "premap/PE/{sample}_{rn}.unmap.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/premap/PE/{sample}_{rn}_R1.fq.gz"),
        r2=temp(TEMPDIR / "unmapped/premap/PE/{sample}_{rn}_R2.fq.gz"),
    shell:
        """
        {PATH.samtools} fastq -1 {output.r1} -2 {output.r2} -0 /dev/null -s /dev/null -n {input}
        """


rule premap_get_unmapped_se:
    benchmark:
        BENCHDIR / "premap_get_unmapped_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "premap_get_unmapped_se.benchmark.txt"
    input:
        un=TEMPDIR / "premap/SE/{sample}_{rn}.unmap.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/premap/SE/{sample}_{rn}_R1.fq.gz"),
    shell:
        """
        {PATH.samtools} fastq -0 {output.r1} -n {input}
        """


# main mapping step (genes and transcript simutaneously if genes provided, otherwise just transcript)


rule mainmap_align_pe:
    benchmark:
        BENCHDIR / "mainmap_align_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "mainmap_align_pe.benchmark.txt"
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
    output:
        mp2=temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.transcript.bam"),
        um=temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.main.bam"),
        summary=temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.summary"),
        **({"mp1": temp(TEMPDIR / "mainmap/PE/{sample}_{rn}.genes.bam")} if HAS_GENES else {})
    threads: 48
    shell:
        "{PATH.coralsnake} map -t {threads} "
        + ("-r {input.rf1} " if HAS_GENES else "")
        + "-r {input.rf2} -1 {input.fq1} -2 {input.fq2} "
        + ("-o {output.mp1} " if HAS_GENES else "")
        + "-o {output.mp2} -u {output.um} && "
        "touch {output.summary}"


rule mainmap_align_se:
    benchmark:
        BENCHDIR / "mainmap_align_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "mainmap_align_se.benchmark.txt"
    input:
        fq=lambda wildcards: (
            TEMPDIR / f"unmapped/premap/SE/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
            if HAS_CONTAM
            else TEMPDIR / f"trim/SE/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
        ),
        rf1=lambda wildcards: INTERNALDIR / "ref/genes.fa" if HAS_GENES else [],
        rf2=INTERNALDIR / "ref/transcript.fa",
    output:
        mp2=temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.transcript.bam"),
        um=temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.main.bam"),
        summary=temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.summary"),
        **({"mp1": temp(TEMPDIR / "mainmap/SE/{sample}_{rn}.genes.bam")} if HAS_GENES else {})
    threads: 48
    shell:
        "{PATH.coralsnake} map -t {threads} "
        + ("-r {input.rf1} " if HAS_GENES else "")
        + "-r {input.rf2} -1 {input.fq} "
        + ("-o {output.mp1} " if HAS_GENES else "")
        + "-o {output.mp2} -u {output.um} && "
        "touch {output.summary}"


rule finalize_mainmap_summary:
    benchmark:
        BENCHDIR / "finalize_mainmap_summary_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_mainmap_summary.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"mainmap/PE/{wildcards.sample}_{wildcards.rn}.summary"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"mainmap/SE/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/mainmap/{sample}_{rn}.summary",
    shell:
        "cp {input} {output}"


if HAS_GENES:
    rule finalize_mainmap_genes_bam:
        input:
            lambda wildcards: (
                TEMPDIR
                / f"mainmap/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.genes.bam"
            ),
        output:
            INTERNALDIR / "bam/per_run/{sample}_{rn}.genes.bam",
        threads: 32
        shell:
            "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule finalize_mainmap_transcript_bam:
    benchmark:
        BENCHDIR / "finalize_mainmap_transcript_bam_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_mainmap_transcript_bam.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR
            / f"mainmap/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.transcript.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.transcript.bam",
    threads: 32
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule mainmap_get_unmapped_pe:
    benchmark:
        BENCHDIR / "mainmap_get_unmapped_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "mainmap_get_unmapped_pe.benchmark.txt"
    input:
        un=TEMPDIR / "mainmap/PE/{sample}_{rn}.main.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R1.fq.gz"),
        r2=temp(TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R2.fq.gz"),
    shell:
        """
        {PATH.samtools} fastq -1 {output.r1} -2 {output.r2} -0 /dev/null -s /dev/null -n {input}
        """


rule mainmap_get_unmapped_se:
    benchmark:
        BENCHDIR / "mainmap_get_unmapped_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "mainmap_get_unmapped_se.benchmark.txt"
    input:
        un=TEMPDIR / "mainmap/SE/{sample}_{rn}.main.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/mainmap/SE/{sample}_{rn}_R1.fq.gz"),
    shell:
        """
        {PATH.samtools} fastq -0 {output.r1} -n {input}
        """


# postmap to genome


rule remap_align_pe:
    benchmark:
        BENCHDIR / "remap_align_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_align_pe.benchmark.txt"
    input:
        fq1=TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R1.fq.gz",
        fq2=TEMPDIR / "unmapped/mainmap/PE/{sample}_{rn}_R2.fq.gz",
    output:
        bam=temp(TEMPDIR / "remap/PE/{sample}_{rn}.genome.bam"),
        summary=temp(TEMPDIR / "remap/PE/{sample}_{rn}.summary"),
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
    threads: 36
    shell:
        """
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -1 {input.fq1} -2 {input.fq2} --base-change {params.basechange} {params.directional} {params.splice_args} \
            --avoid-pseudogene --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-3,-0.5 |\
            {PATH.samtools} view -@ {threads} -O BAM -o {output.bam}
        """


rule remap_align_se:
    benchmark:
        BENCHDIR / "remap_align_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_align_se.benchmark.txt"
    input:
        fq=TEMPDIR / "unmapped/mainmap/SE/{sample}_{rn}_R1.fq.gz",
    output:
        bam=temp(TEMPDIR / "remap/SE/{sample}_{rn}.genome.bam"),
        summary=temp(TEMPDIR / "remap/SE/{sample}_{rn}.summary"),
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
    threads: 36
    shell:
        """
        {PATH.hisat3n} --index {params.index} -p {threads} --summary-file {output.summary} --new-summary -q -U {input.fq} --base-change {params.basechange} {params.directional} {params.splice_args} \
            --avoid-pseudogene --np 0 --rdg 5,3 --rfg 5,3 --sp 9,3 --mp 3,1 --score-min L,-3,-0.5 |\
            {PATH.samtools} view -@ {threads} -o {output.bam}
        """


rule finalize_remap_summary:
    benchmark:
        BENCHDIR / "finalize_remap_summary_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_remap_summary.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"remap/PE/{wildcards.sample}_{wildcards.rn}.summary"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"remap/SE/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/remap/{sample}_{rn}.summary",
    shell:
        "cp {input} {output}"


rule remap_fixmate_pe:
    benchmark:
        BENCHDIR / "remap_fixmate_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_fixmate_pe.benchmark.txt"
    input:
        TEMPDIR / "remap/PE/{sample}_{rn}.genome.bam",
    output:
        temp(TEMPDIR / "remap/PE/{sample}_{rn}.fixmate.bam"),
    threads: 8
    shell:
        "{PATH.samtools} fixmate -@ {threads} -m -O BAM {input} {output}"


rule remap_fixmate_se:
    benchmark:
        BENCHDIR / "remap_fixmate_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_fixmate_se.benchmark.txt"
    input:
        TEMPDIR / "remap/SE/{sample}_{rn}.genome.bam",
    output:
        temp(TEMPDIR / "remap/SE/{sample}_{rn}.fixmate.bam"),
    threads: 8
    shell:
        "cp {input} {output}"


rule remap_tag_pe:
    benchmark:
        BENCHDIR / "remap_tag_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_tag_pe.benchmark.txt"
    input:
        TEMPDIR / "remap/PE/{sample}_{rn}.fixmate.bam",
    output:
        temp(TEMPDIR / "remap/PE/{sample}_{rn}.tagged.bam"),
    params:
        strand=lambda wildcards: (
            "0" if SAMPLE2LIB[wildcards.sample] == "UNSTRANDED" else "1"
        ),
    threads: 32
    shell:
        "{PATH.bam_tag} {input} {output} --threads {threads} --strand-type {params.strand}"


rule remap_tag_se:
    benchmark:
        BENCHDIR / "remap_tag_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_tag_se.benchmark.txt"
    input:
        TEMPDIR / "remap/SE/{sample}_{rn}.fixmate.bam",
    output:
        temp(TEMPDIR / "remap/SE/{sample}_{rn}.tagged.bam"),
    params:
        strand=lambda wildcards: (
            "0" if SAMPLE2LIB[wildcards.sample] == "UNSTRANDED" else "1"
        ),
    threads: 32
    shell:
        "{PATH.bam_tag} {input} {output} --threads {threads} --strand-type {params.strand}"


rule remap_filter_sort_pe:
    benchmark:
        BENCHDIR / "remap_filter_sort_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_filter_sort_pe.benchmark.txt"
    input:
        TEMPDIR / "remap/PE/{sample}_{rn}.tagged.bam",
    output:
        unmap=temp(TEMPDIR / "remap/PE/{sample}_{rn}.final_unmap.bam"),
        mapped=temp(TEMPDIR / "remap/PE/{sample}_{rn}.mapped.bam"),
        report=temp(TEMPDIR / "remap/PE/{sample}_{rn}.report.json"),
    threads: 32
    shell:
        """
        {PATH.samtools} view -e 'exists([AP]) && [AP] <= 0.05 && !flag.secondary' -@ {threads} -U {output.unmap} --save-counts {output.report} -h {input} |\
            {PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output.mapped}
        """


rule remap_filter_sort_se:
    benchmark:
        BENCHDIR / "remap_filter_sort_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_filter_sort_se.benchmark.txt"
    input:
        TEMPDIR / "remap/SE/{sample}_{rn}.tagged.bam",
    output:
        unmap=temp(TEMPDIR / "remap/SE/{sample}_{rn}.final_unmap.bam"),
        mapped=temp(TEMPDIR / "remap/SE/{sample}_{rn}.mapped.bam"),
        report=temp(TEMPDIR / "remap/SE/{sample}_{rn}.report.json"),
    threads: 32
    shell:
        """
        {PATH.samtools} view -e 'exists([AP]) && [AP] <= 0.05 && !flag.secondary' -@ {threads} -U {output.unmap} --save-counts {output.report} -h {input} |\
            {PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output.mapped}
        """


rule finalize_genome_bam:
    benchmark:
        BENCHDIR / "finalize_genome_bam_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_genome_bam.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"remap/PE/{wildcards.sample}_{wildcards.rn}.mapped.bam"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"remap/SE/{wildcards.sample}_{wildcards.rn}.mapped.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.genome.bam",
    shell:
        "cp {input} {output}"


rule finalize_genome_report:
    benchmark:
        BENCHDIR / "finalize_genome_report_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_genome_report.benchmark.txt"
    input:
        lambda wildcards: (
            TEMPDIR / f"remap/PE/{wildcards.sample}_{wildcards.rn}.report.json"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR / f"remap/SE/{wildcards.sample}_{wildcards.rn}.report.json"
        ),
    output:
        INTERNALDIR / "stats/filter/{sample}_{rn}.genome.json",
    shell:
        "cp {input} {output}"


rule remap_get_unmapped_pe:
    benchmark:
        BENCHDIR / "remap_get_unmapped_pe_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_get_unmapped_pe.benchmark.txt"
    input:
        un=TEMPDIR / "remap/PE/{sample}_{rn}.final_unmap.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/remap/PE/{sample}_{rn}_R1.fq.gz"),
        r2=temp(TEMPDIR / "unmapped/remap/PE/{sample}_{rn}_R2.fq.gz"),
    shell:
        """
        {PATH.samtools} fastq -1 {output.r1} -2 {output.r2} -0 /dev/null -s /dev/null -n {input}
        """


rule remap_get_unmapped_se:
    benchmark:
        BENCHDIR / "remap_get_unmapped_se_{sample}_{rn}.benchmark.txt"
    benchmark:
        BENCHDIR / "remap_get_unmapped_se.benchmark.txt"
    input:
        un=TEMPDIR / "remap/SE/{sample}_{rn}.final_unmap.bam",
    output:
        r1=temp(TEMPDIR / "unmapped/remap/SE/{sample}_{rn}_R1.fq.gz"),
    shell:
        """
        {PATH.samtools} fastq -0 {output.r1} -n {input}
        """


rule finalize_unmapped_fq:
    benchmark:
        BENCHDIR / "finalize_unmapped_fq_{sample}_{rn}_{rd}.benchmark.txt"
    benchmark:
        BENCHDIR / "finalize_unmapped_fq.benchmark.txt"
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
    shell:
        "cp {input} {output}"


rule unmapped_qc:
    benchmark:
        BENCHDIR / "unmapped_qc_{sample}_{rn}_{rd}.benchmark.txt"
    benchmark:
        BENCHDIR / "unmapped_qc.benchmark.txt"
    input:
        INTERNALDIR / "fastq/unmapped/{sample}_{rn}_{rd}.fq.gz",
    output:
        html=INTERNALDIR / "qc/fastqc_unmapped/{sample}_{rn}_{rd}/fastqc_report.html",
        text=INTERNALDIR / "qc/fastqc_unmapped/{sample}_{rn}_{rd}/fastqc_data.txt",
        summary=INTERNALDIR / "qc/fastqc_unmapped/{sample}_{rn}_{rd}/summary.txt",
    params:
        lambda wildcards: INTERNALDIR
        / f"qc/fastqc/unmapped/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}",
    shell:
        "{PATH.falco} -o {params} {input}"


rule unmapped_report:
    benchmark:
        BENCHDIR / "unmapped_report.benchmark.txt"
    benchmark:
        BENCHDIR / "unmapped_report.benchmark.txt"
    input:
        [
            INTERNALDIR / f"qc/fastqc/unmapped/{s}_{r}_{i}/fastqc_data.txt"
            for s, v in SAMPLE2DATA.items()
            for r, v2 in v.items()
            for i in ["R1", "R2"]
            if i in v2 or (i == "R2" and len(v2) == 2)
        ],
    output:
        "report_reads/unmapped.html",
    shell:
        "{PATH.multiqc} -f -m fastqc -n {output} {input}"


#######################
# combine runs
#######################


rule combine_bams:
    benchmark:
        BENCHDIR / "combine_bams_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "combine_bams.benchmark.txt"
    input:
        lambda wildcards: [
            INTERNALDIR / f"run_bam/{wildcards.sample}_{r}.{wildcards.reftype}.bam"
            for r in SAMPLE2DATA[wildcards.sample]
        ],
    output:
        bam=temp(TEMPDIR / "combined/{sample}.{reftype}.bam"),
        bai=temp(TEMPDIR / "combined/{sample}.{reftype}.bam.bai"),
    threads: 32
    shell:
        """
        {PATH.samtools} merge -@ {threads} -f --write-index -o {output.bam}##idx##{output.bai} {input}
        """


rule stat_combined:
    benchmark:
        BENCHDIR / "stat_combined_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "stat_combined.benchmark.txt"
    input:
        bam=TEMPDIR / "combined/{sample}.{reftype}.bam",
    output:
        stat=INTERNALDIR / "stats/combined/{sample}.{reftype}.txt",
        n=INTERNALDIR / "stats/combined/{sample}.{reftype}.count",
    threads: 4
    shell:
        """
        {PATH.samtools} flagstat -@ {threads} -O TSV {input} > {output.stat}
        {PATH.samtools} view -@ {threads} -c -F 384 {input} > {output.n}
        """


rule drop_duplicates:
    benchmark:
        BENCHDIR / "drop_duplicates_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "drop_duplicates.benchmark.txt"
    input:
        bam=TEMPDIR / "combined/{sample}.{reftype}.bam",
        bai=TEMPDIR / "combined/{sample}.{reftype}.bam.bai",
    output:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
        txt=INTERNALDIR / "stats/dedup/{sample}.{reftype}.log",
    threads: 48
    shell:
        "{PATH.markdup} -t {threads} -i {input.bam} -o {output.bam} --report {output.txt}"


rule dedup_index:
    benchmark:
        BENCHDIR / "dedup_index_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "dedup_index.benchmark.txt"
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        bai=INTERNALDIR / "bam/{sample}.{reftype}.bam.bai",
    threads: 12
    shell:
        "{PATH.samtools} index -@ {threads} {input}"


rule stat_dedup:
    benchmark:
        BENCHDIR / "stat_dedup_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "stat_dedup.benchmark.txt"
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        stat=INTERNALDIR / "stats/dedup/{sample}.{reftype}.txt",
        n=INTERNALDIR / "stats/dedup/{sample}.{reftype}.count",
    threads: 4
    shell:
        """
        {PATH.samtools} flagstat -@ {threads} -O TSV {input} > {output.stat}
        {PATH.samtools} view -@ {threads} -c -F 384 {input} > {output.n}
        """


rule liftover_transcript_to_genome:
    benchmark:
        BENCHDIR / "liftover_transcript_to_genome_{sample}.benchmark.txt"
    benchmark:
        BENCHDIR / "liftover_transcript_to_genome.benchmark.txt"
    input:
        transcripts=INTERNALDIR / "bam/{sample}.transcript.bam",
        genome=INTERNALDIR / "bam/{sample}.genome.bam",
        info=INTERNALDIR / "ref/transcript.tsv",
    output:
        transcripts=temp(TEMPDIR / "liftover/{sample}.transcript.bam"),
        bam=INTERNALDIR / "liftover_bam/{sample}.bam",
    params:
        fai=REF["genome"]["fa"] + ".fai",
    threads: 24
    shell:
        """
        coralsnake liftover -t {threads} -i {input.transcripts} -o {output.transcripts} -a {input.info} -f {params.fai}
        {PATH.samtools} cat {output.transcripts} {input.genome} | {PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output.bam}
        """


rule count_reads:
    benchmark:
        BENCHDIR / "count_reads_{sample}.benchmark.txt"
    benchmark:
        BENCHDIR / "count_reads.benchmark.txt"
    input:
        report=lambda wildcards: [
            INTERNALDIR / f"qc/trimming/{wildcards.sample}_{r}.json"
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
        count3=(INTERNALDIR / "stats/combined/{sample}.genes.count" if HAS_GENES else []),
        count4=(INTERNALDIR / "stats/dedup/{sample}.genes.count" if HAS_GENES else []),
        count5=INTERNALDIR / "stats/combined/{sample}.transcript.count",
        count6=INTERNALDIR / "stats/dedup/{sample}.transcript.count",
        count7=INTERNALDIR / "stats/combined/{sample}.genome.count",
        count8=INTERNALDIR / "stats/dedup/{sample}.genome.count",
    output:
        INTERNALDIR / "stats/count/{sample}.tsv",
    threads: 2
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
    benchmark:
        BENCHDIR / "insert_size_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "insert_size.benchmark.txt"
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        tsv=INTERNALDIR / "stats/rlen/{sample}.{reftype}.isize.tsv",
    threads: 8
    shell:
        """
        {PATH.samtools} stats -@ {threads} -i 1000 {input} |grep ^IS|cut -f 2- > {output}
        """


rule read_length:
    benchmark:
        BENCHDIR / "read_length_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "read_length.benchmark.txt"
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
    output:
        tsv=INTERNALDIR / "stats/rlen/{sample}.{reftype}.rlen.tsv",
    threads: 8
    shell:
        """
        {PATH.samtools} stats -@ {threads} -i 1000 {input} |grep ^RL | cut -f 2- > {output}
        """


###################
# call sites
###################


if HAS_GENES:
    rule cal_spike_ratio:
        input:
            bam=expand(
                INTERNALDIR / "bam/{sample}.genes.bam", sample=SAMPLE2DATA.keys()
            ),
            bai=expand(
                INTERNALDIR / "bam/{sample}.genes.bam.bai",
                sample=SAMPLE2DATA.keys(),
            ),
        output:
            tsv=INTERNALDIR / "stats/ratio/probe.tsv",
        threads: 8
        shell:
            """
            {PATH.bam_conv} {input.bam} > {output}
            """


rule run_countmut:
    benchmark:
        BENCHDIR / "run_countmut_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "run_countmut.benchmark.txt"
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
    threads: 32
    params:
        ref_base=lambda wildcards: "C" if config.get("pileup_ct", False) else "A",
        mut_base=lambda wildcards: "T" if config.get("pileup_ct", False) else "G",
    shell:
        "{PATH.countmut} -i {input.bam} -r {input.ref} -o {output} -t {threads} --ref-base {params.ref_base} --mut-base {params.mut_base} -f > /dev/null"


rule pileup_base:
    benchmark:
        BENCHDIR / "pileup_base_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "pileup_base.benchmark.txt"
    input:
        TEMPDIR / "pileup/{sample}.{reftype}.tsv",
    output:
        INTERNALDIR / "pileup/{sample}.{reftype}.tsv.gz",
    threads: 16
    shell:
        "{PATH.bgzip} -@ {threads} -c {input} > {output}"


rule unfilter_genes_stat:
    benchmark:
        BENCHDIR / "unfilter_genes_stat_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "unfilter_genes_stat.benchmark.txt"
    input:
        INTERNALDIR / "pileup/{sample}.{reftype}.tsv.gz",
    output:
        INTERNALDIR / "stats/{sample}.{reftype}.genes.tsv",
    shell:
        """
        zcat {input} | awk -F '\\t' 'NR>1 && $1!~"^probe_" && ($6+$9+$7+$10+0)>0{{u[$6]+=$6+$7; d[$1]+=$6+$9+$7+$10; r[$1]+=($6+$7)/($6+$9+$7+$10); n[$1]+=1}}END{{ for(x in u){{print x,n[x],u[x],d[x],r[x]/n[x]}} }}' > {output}
        """


rule motif_conversion_rate_stat:
    benchmark:
        BENCHDIR / "motif_conversion_rate_stat_{sample}_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "motif_conversion_rate_stat.benchmark.txt"
    input:
        pileup=INTERNALDIR / "pileup/{sample}.{reftype}.tsv.gz",
    output:
        INTERNALDIR / "stats/{sample}.{reftype}.motif.tsv",
    shell:
        'zcat {input.pileup} | awk -F \'\\t\' \'BEGIN{{OFS="\\t";print "Motif","Count","Unconverted","Depth","Ratio"}} NR>1 && ($6+$9+$7+$10+0)>0{{m=toupper(substr($4,15,3)); if(m ~ /^[ATGC]+$/){{n[m]+=1;u[m]+=$6+$7;d[m]+=$6+$9+$7+$10;r[m]+=($6+$7)/($6+$9+$7+$10)}}}} END{{for(m in d) print m,n[m],u[m],d[m],r[m]/n[m]}}\' > {output}'


rule join_pileup_table:
    benchmark:
        BENCHDIR / "join_pileup_table_{reftype}.benchmark.txt"
    benchmark:
        BENCHDIR / "join_pileup_table.benchmark.txt"
    input:
        expand(
            INTERNALDIR / "pileup/{sample}.{{reftype}}.tsv.gz",
            sample=SAMPLE2DATA.keys(),
        ),
    output:
        "report_sites/{reftype}.tsv.gz",
    params:
        samples=" ".join(SAMPLE2DATA.keys()),
        requires=" ".join(
            [
                (
                    "0"
                    if s in SKIP_SAMPLES
                    else "1"
                )
                for s in SAMPLE2DATA.keys()
            ]
        ),
    threads: lambda wildcards, input: min(int(len(input) * 4), 64)
    shell:
        """
        {PATH.merge_samples} --files {input} --names {params.samples} --output {output} --requires {params.requires}
        """


rule merge_gene_and_genome_table:
    benchmark:
        BENCHDIR / "merge_gene_and_genome_table.benchmark.txt"
    benchmark:
        BENCHDIR / "merge_gene_and_genome_table.benchmark.txt"
    input:
        info=INTERNALDIR / "ref/transcript.tsv",
        transcripts="report_sites/transcript.tsv.gz",
        genome="report_sites/genome.tsv.gz",
    output:
        "report_sites/merged.tsv.gz",
    threads: 48
    shell:
        """
        {PATH.remap_genome} -t {input.info} -a {input.transcripts} -b {input.genome} -o {output} --min-depth {config[min_merged_depth]}
        """


rule filter_eTAM_sites:
    benchmark:
        BENCHDIR / "filter_eTAM_sites.benchmark.txt"
    benchmark:
        BENCHDIR / "filter_eTAM_sites.benchmark.txt"
    input:
        "report_sites/merged.tsv.gz",
    output:
        fl="report_sites/filtered.tsv",
    threads: 48
    shell:
        """
        {PATH.filter_sites} -i {input} -o {output.fl}
        """


rule group_and_pval_cal:
    benchmark:
        BENCHDIR / "group_and_pval_cal_{group}.benchmark.txt"
    benchmark:
        BENCHDIR / "group_and_pval_cal.benchmark.txt"
    input:
        "report_sites/merged.tsv.gz",
    output:
        "report_sites/grouped/{group}.parquet",
    params:
        names=lambda wildcards: GROUP2SAMPLE[wildcards.group],
    threads: 24
    shell:
        """
        {PATH.sum_groups} -i {input} -o {output} -n {params.names}
        """


###################
# multiqc custom
###################


rule aggregate_multiqc_stats:
    benchmark:
        BENCHDIR / "aggregate_multiqc_stats.benchmark.txt"
    benchmark:
        BENCHDIR / "aggregate_multiqc_stats.benchmark.txt"
    input:
        counts=expand(
            INTERNALDIR / "stats/count/{sample}.tsv", sample=SAMPLE2DATA.keys()
        ),
        motifs=expand(
            INTERNALDIR / "stats/{sample}.transcript.motif.tsv",
            sample=SAMPLE2DATA.keys(),
        ),
        dedup_logs=expand(
            INTERNALDIR / "stats/dedup/{sample}.genome.log",
            sample=SAMPLE2DATA.keys(),
        ),
        trim_jsons=expand(
            INTERNALDIR / "qc/trimming/{sample}_{rn}.json",
            sample=SAMPLE2DATA.keys(),
            rn=["run1"],
        ),
    output:
        mapping=INTERNALDIR / "stats/multiqc/mapping_stats_mqc.tsv",
        motifs=INTERNALDIR / "stats/multiqc_sites/motif_conversion_mqc.tsv",
        dedup=INTERNALDIR / "stats/multiqc/dedup_stats_mqc.tsv",
    shell:
        """
        {PATH.mqc_mapping} {output.mapping} {output.dedup} {input.counts} --dedup-logs {input.dedup_logs} --trim-jsons {input.trim_jsons}
        {PATH.mqc_sites} {output.motifs} {input.motifs}
        """


rule generate_mapping_report:
    benchmark:
        BENCHDIR / "generate_mapping_report.benchmark.txt"
    benchmark:
        BENCHDIR / "generate_mapping_report.benchmark.txt"
    input:
        INTERNALDIR / "stats/multiqc/mapping_stats_mqc.tsv",
        INTERNALDIR / "stats/multiqc/dedup_stats_mqc.tsv",
    output:
        "report_reads/mapping.html",
    params:
        report_name="mapping.html",
        report_dir=str(Path("report_reads")),
        search_dir=str(INTERNALDIR / "stats/multiqc"),
    shell:
        "{PATH.multiqc} -f -n {params.report_name} -o {params.report_dir} {params.search_dir}"


rule generate_site_report:
    benchmark:
        BENCHDIR / "generate_site_report.benchmark.txt"
    benchmark:
        BENCHDIR / "generate_site_report.benchmark.txt"
    input:
        INTERNALDIR / "stats/multiqc_sites/motif_conversion_mqc.tsv",
    output:
        "report_sites/sites.html",
    params:
        report_name="sites.html",
        report_dir=str(Path("report_sites")),
        search_dir=str(INTERNALDIR / "stats/multiqc_sites"),
    shell:
        "{PATH.multiqc} -f -n {params.report_name} -o {params.report_dir} {params.search_dir}"


###################
# multiqc custom
###################
