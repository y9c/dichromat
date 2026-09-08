"""dichromat pipeline DAG.

General conversion-based RNA-seq pipeline (eTAM-seq / CAM-seq / GLORI /
BS-seq, etc.).  High-level phases:

  1. Reference preparation
     prepare_reference (user FASTA combine + GTF-derived via coralsnake prepare)
  2. Trimming & read QC
     trim_se / trim_pe / qc_trimmed / report_qc_trimmed
  3. Competitive mapping cascade
     map_cascade (one prismalign call, all layers)
  4. BAM merge + dedup + stats
     finalize_map_bam / finalize_map_summary / combine_bams / drop_duplicates
  5. Site calling
     run_countmut / pileup_base / join_pileup / merge_sites
  6. Site/read report generation
     filter_sites / annotate_sites / mqc_aggregate_* / generate_*_report

All rules share the resolved `config`, `PATH` (SimpleNamespace of tool
commands) and path constants (INTERNALDIR / TEMPDIR / BENCHDIR).
"""

from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
import os
import yaml


# ---------------------------------------------------------------------------
# Config loading & merging
# ---------------------------------------------------------------------------

# Start from the packaged defaults, then layer the user config on top.
with open(Path(workflow.basedir) / "default.yaml") as f:
    merged_config = yaml.safe_load(f)

# `path` and `reference` are deep-merged (dict.update); everything else is a
# plain top-level override.
user_config = dict(config)
user_path = user_config.get("path", {})

for k, v in user_config.items():
    if k == "path" and isinstance(v, dict):
        merged_config["path"].update(v)
    elif k == "reference" and isinstance(v, dict):
        merged_config.setdefault("reference", {}).update(v)
    else:
        merged_config[k] = v

config = merged_config

# ---------------------------------------------------------------------------
# Batch / sample metadata
# ---------------------------------------------------------------------------

BATCH = config.get("batch", "dichromat_run")
SKIP_SAMPLES = config.get("skip_samples", [])


# ---------------------------------------------------------------------------
# Container handling
# ---------------------------------------------------------------------------

# Detect whether we are already running inside the dichromat container.
INSIDE_CONTAINER = os.environ.get("PIPELINE_HOME") == "/pipeline"

# When using a container, the tool commands baked into the container PATH are
# authoritative, so we revert `path` to the packaged defaults.  Any explicit
# user `path` overrides are preserved (e.g. a bind-mounted repo copy of a
# Python script that fixes a stale-container bug).
if config.get("container") or INSIDE_CONTAINER:
    with open(Path(workflow.basedir) / "default.yaml") as f:
        clean_defaults = yaml.safe_load(f)
        user_path = config.get("path", {})
        config["path"] = clean_defaults.get("path", {})
        config["path"].update(user_path)

# Resolve a relative container path against the Snakefile directory.
CONTAINER = config.get("container")
if CONTAINER and not os.path.isabs(CONTAINER):
    CONTAINER = os.path.normpath(os.path.join(workflow.basedir, CONTAINER))

# Container directive for every rule.  If already inside the container we MUST
# set this to None to avoid nesting.
container: None if INSIDE_CONTAINER else CONTAINER


def resolve_config_path(p):
    """Resolve a (possibly relative / ~-expanded) path to an absolute path."""
    if not p or not isinstance(p, str) or os.path.isabs(p):
        return p
    p = os.path.expanduser(p)
    # 1. Relative to CWD (user workspace or where the command was run).
    if os.path.exists(p):
        return os.path.abspath(p)
    # 2. Relative to the project root (from dichromat.sh or the Snakefile dir).
    base = config.get("project_dir", workflow.basedir)
    p_joined = os.path.join(base, p)
    if os.path.exists(p_joined):
        return os.path.normpath(p_joined)
    # 3. Fallback to abspath from CWD.
    return os.path.abspath(p)


# ---------------------------------------------------------------------------
# Global path constants & flags
# ---------------------------------------------------------------------------

# References.  Two accepted forms:
#   * list of {key, fa/gtf, ...}  (preferred, prismalign key: style)
#   * dict keyed by layer name       (legacy, e.g. {genome: {fa: ...}})
# Both are normalised into REF: {key: {fa, gtf, hisat3n, liftover, ...}}.
_raw_ref = config.get("reference", {})
REF = {}
if isinstance(_raw_ref, list):
    for entry in _raw_ref:
        if not isinstance(entry, dict) or "key" not in entry:
            raise SystemExit("each 'reference' entry needs a 'key'")
        k = str(entry["key"])
        REF[k] = {kk: vv for kk, vv in entry.items() if kk != "key"}
else:
    # Legacy dict form: {genome: {fa: ...}, contamination: [...]}
    for k, v in _raw_ref.items():
        REF[str(k)] = v if isinstance(v, dict) else {"fa": v}

# Expand user paths and resolve relative paths in every reference value.
def _resolve_ref_value(v):
    if isinstance(v, dict):
        return {kk: resolve_config_path(vv) for kk, vv in v.items()}
    if isinstance(v, list):
        return [resolve_config_path(x) for x in v]
    return resolve_config_path(v)

for k in REF:
    REF[k] = _resolve_ref_value(REF[k])

TEMPDIR = Path(config.get("tempdir", ".tmp"))
# `path` dict -> SimpleNamespace for dot access (PATH.python, PATH.samtools...).
PATH = SimpleNamespace(**config.get("path", {}))

INTERNALDIR = Path("internal_files")
BENCHDIR = Path(".snakemake/benchmarks")
MARKDUP = config.get("markdup", True)


wildcard_constraints:
    sample=r"[^/\.]+",
    rn=r"run[0-9]+",
    reftype="genome|transcript|genes|contamination",
    libmode="PE|SE",


# ---------------------------------------------------------------------------
# Sample parsing (supports both `samples` and `samples_<BATCH>`)
# ---------------------------------------------------------------------------

SAMPLE2DATA = defaultdict(lambda: defaultdict(dict))
GROUP2SAMPLE = defaultdict(list)
SAMPLE2ADAPTER = defaultdict(str)

samples_dict = config.get("samples") or config.get(f"samples_{BATCH}")
if not samples_dict:
    raise SystemExit(f"Please add 'samples' or 'samples_{BATCH}' in your config file")

for s, v in samples_dict.items():
    s = str(s)
    # Unified adapter scheme: a built-in cutseq name (TAKARAV2, ECLIP10, ...)
    # or a custom grammar string, passed to cutseq via -A/--adapter-scheme.
    SAMPLE2ADAPTER[s] = v.get("adapter", config.get("adapter", ""))
    if "group" in v:
        GROUP2SAMPLE[v["group"]].append(s)
    for i, v2 in enumerate(v["data"], 1):
        r = f"run{i}"
        SAMPLE2DATA[str(s)][r] = {
            k: os.path.expanduser(v3) for k, v3 in dict(v2).items()
        }

# The prismalign pipeline YAML is the single source of truth for which mapping
# layers run (and their order).  Parse it here so the Snakefile's reftypes,
# reference binding and downstream rules all follow the declared layers
# instead of hard-coding a transcript+genome cascade.  A genome-only run (e.g.
# bacteria with no spliced transcriptome) uses a pipeline YAML with just the
# ``genome`` layer and the pipeline automatically skips the transcript layer
# (reference build, index, mapping, liftover, merge).
# The prismalign mapping layers are declared inline in the config (``mapping:``
# block), so there is no separate pipeline YAML to maintain.  The Snakefile
# derives the active reftypes here and generates the prismalign pipeline YAML
# that the map_cascade rule consumes (see rule prepare_mapping).
_mapping_cfg = config.get("mapping", {})
if not isinstance(_mapping_cfg, dict) or not _mapping_cfg.get("layers"):
    raise SystemExit("config 'mapping' block missing or has no 'layers'")
_pipeline_layers = [l for l in _mapping_cfg.get("layers", []) if l.get("key")]
# The generated prismalign pipeline YAML path (written by prepare_mapping
# into the run/workspace directory, so each run gets its own copy).
PIPELINE_PATH = "mapping.generated.yaml"
LAYER_KEYS = [str(l.get("key")) for l in _pipeline_layers]

# Derive the base-change / secondary-change (the conversion chemistry) from the
# first layer's ``mutation_classes`` (single source of truth).  base_change is
# the comma-joined source bases (e.g. "A,C"); secondary_change the targets
# (e.g. "G,T").  Falls back to the (now-removed) config keys for compatibility.
_first_mut_classes = next(
    (l.get("mutation_classes") for l in _pipeline_layers if l.get("mutation_classes")),
    None,
)
if _first_mut_classes:
    BASE_CHANGE = ",".join(str(c.get("source")) for c in _first_mut_classes)
    SECONDARY_CHANGE = ",".join(str(c.get("target")) for c in _first_mut_classes)
else:
    BASE_CHANGE = config.get("base_change", "A,G")
    SECONDARY_CHANGE = config.get("secondary_change", "")

# Global alignment filters (used by map_cascade / countmut).  The per-layer
# ``filter:`` in the mapping block overrides these for the layer that declares
# them.
MIN_MAPPING_RATIO = config.get("min_mapping_ratio", 0.8)
MAX_MISMATCHES = config.get("max_mismatches", 2)

# countmut read-gate parameters (the -e group router).  Consolidated into a
# single ``countmut:`` block; defaults mirror the legacy 0.0.8 gate.
COUNTMUT = config.get("countmut", {})
COUNTMUT_MAX_SUB = COUNTMUT.get("max_sub", 1)
COUNTMUT_MIN_CON = COUNTMUT.get("min_con", 1)
COUNTMUT_MAX_UNC = COUNTMUT.get("max_unc", 3)
COUNTMUT_MIN_BASEQ = COUNTMUT.get("min_baseq", 20)
COUNTMUT_TRIM = COUNTMUT.get("trim", 2)
# Active mapping layers (single source of truth from the mapping block).  Use
# ``has_layer(key)`` / the ``ACTIVE_LAYERS`` set instead of the old scattered
# HAS_TRANSCRIPT / HAS_GENES / HAS_CONTAM booleans.
ACTIVE_LAYERS = set(LAYER_KEYS)

def has_layer(key: str) -> bool:
    return key in ACTIVE_LAYERS
HAS_GENOME = "genome" in LAYER_KEYS
# The genome layer's engine decides how its reference is bound: a spliced
# hisat3n genome layer uses a prebuilt ``.3n`` index prefix, while a plain
# bwa-mem2 genome layer (e.g. genome-only bacteria) is bound by FASTA only.
_GENOME_LAYER = next((l for l in _pipeline_layers if l.get("key") == "genome"), {})
GENOME_ENGINE = str(_GENOME_LAYER.get("engine", ""))
GENOME_HAS_HISAT3N = "hisat3n" in GENOME_ENGINE

# Active reftypes in pipeline-layer order (contamination/genes are the
# optional pre/main masks; transcript+genome the main mapping layers).
REFTYPES = LAYER_KEYS

# Site-calling reftypes: the layers that produce sites (declared with
# ``site: true`` in the pipeline YAML).  Defaults to the main mapping layers
# (transcript + genome) when no layer declares ``site``.
_site_layers = [str(l.get("key")) for l in _pipeline_layers if l.get("site")]
SITE_REFTYPES = _site_layers or [
    r for r in LAYER_KEYS if r in ("transcript", "genome")
]

# Map each layer key to its reference FASTA path.  The reference for a layer is
# the prepared file in internal_files/ref/ (for user FASTA or GTF-derived
# references) or the external genome FASTA.  This is the single source for
# ``-r key=path`` bindings and for run_countmut's per-reftype reference
# resolution.
def _layer_ref_fa(key: str) -> str:
    ref = REF.get(key)
    if ref is None:
        raise KeyError(f"no reference for layer key {key!r}")
    # A GTF-derived reference is prepared into internal_files/ref/<key>.fa.
    if ref.get("gtf") or ref.get("liftover"):
        return str(INTERNALDIR / f"ref/{key}.fa")
    # User-supplied FASTA (single path or list -> combined into <key>.fa).
    fa = ref.get("fa")
    if isinstance(fa, list):
        return str(INTERNALDIR / f"ref/{key}.fa")
    if fa:
        return fa
    raise KeyError(f"reference {key!r} has no fa or gtf")

REF_BY_LAYER = {k: _layer_ref_fa(k) for k in LAYER_KEYS}

# Map a layer key to the map_cascade output attribute holding its BAM.
_OUT_ATTR = {"contamination": "contam", "genes": "genes", "transcript": "tx", "genome": "genome"}
def _out_attr(key: str) -> str:
    return _OUT_ATTR[key]

# Map a reftype (layer key) to the BAM filename suffix prismalign emits for
# that layer (used by finalize_map_bam to find the per-layer BAM in TEMPDIR).
_BAM_SUFFIX = {"contamination": "contam", "genes": "genes", "transcript": "transcript", "genome": "genome"}
def _bam_suffix(key: str) -> str:
    return _BAM_SUFFIX[key]

# Build the ``-r key=fa[:index_prefix]`` binding for a layer.  A hisat3n layer
# Build the ``-r key=fa[:index_prefix]`` binding for a layer.  The prebuilt
# index prefix is looked up in the reference under the layer's ENGINE name
# (e.g. ``hisat3n`` for a hisat-3n layer, ``bwa_mem2`` for bwa-mem2), so any
# engine can supply a prebuilt index.  If absent, the FASTA is bound alone and
# prismalign builds the index lazily.
def _layer_ref_binding(key: str) -> str:
    fa = REF_BY_LAYER[key]
    layer = next((l for l in _pipeline_layers if l.get("key") == key), {})
    engine = str(layer.get("engine", ""))
    # Normalise engine name to the reference field (hisat3n -> hisat3n,
    # bwa-mem2 -> bwa_mem2, etc.).
    idx_field = engine.replace("-", "_")
    idx = REF.get(key, {}).get(idx_field) or REF.get(key, {}).get("index")
    if idx:
        return f"{fa}:{idx}"
    return fa


def is_pe(sample, rn):
    """True if the sample/run is paired-end (has both R1 and R2)."""
    return len(SAMPLE2DATA[sample][rn]) == 2


def get_lib_subdir(sample, rn):
    """'PE' or 'SE' subdirectory for the sample/run."""
    return "PE" if is_pe(sample, rn) else "SE"


def is_unstranded(sample):
    """True if the sample's adapter scheme is unstranded.

    The cutseq grammar uses `:` as the unstranded separator (e.g. the built-in
    UNSTRANDED scheme `...XX:XX...`), while `+`/`-` are sense/antisense.
    """
    scheme = SAMPLE2ADAPTER[sample]
    return ":" in scheme or scheme.upper() == "UNSTRANDED"


rule all:
    input:
        "report_reads/mapping.html",
        "report_reads/trimmed.html",
        "report_reads/unmapped.html",
        "report_sites/sites.html",
        "report_sites/filtered.tsv",
        "report_sites/filtered.annotated.tsv",
        expand("report_sites/grouped/{group}.parquet", group=GROUP2SAMPLE.keys()),
        expand(INTERNALDIR / "qc/rnaseq/{sample}.metrics.tsv", sample=SAMPLE2DATA.keys()),
        [
            INTERNALDIR / f"fastq/discarded/{sample}_{rn}_{rd}.fq.gz"
            for sample, v in SAMPLE2DATA.items()
            for rn, v2 in v.items()
            for rd in v2.keys()
        ],
        INTERNALDIR / "README.md",
        INTERNALDIR / "stats/ratio/probe.tsv" if has_layer("genes") else [],
    benchmark:
        BENCHDIR / "all.benchmark.txt"


# ---------------------------------------------------------------------------
# Phase 1: Reference & index preparation
# ---------------------------------------------------------------------------


rule prepare_mapping:
    """Write the prismalign pipeline YAML from the config ``mapping`` block.

    The mapping layers are declared inline in config.yaml (single source of
    truth); this rule serialises them to the pipeline YAML file that the
    map_cascade rule passes to prismalign.  Keeping the layers in config (not
    a hand-maintained YAML) avoids drift between the two.
    """
    output:
        pipeline=PIPELINE_PATH,
    benchmark:
        BENCHDIR / "prepare_mapping.benchmark.txt"
    run:
        import yaml as _yaml
        import os
        cfg = dict(_mapping_cfg)
        cfg.setdefault("threads", 128)
        cfg.setdefault("index_dir", "internal_files/ref/map_index")
        _dir = os.path.dirname(str(output.pipeline))
        if _dir:
            os.makedirs(_dir, exist_ok=True)
        with open(output.pipeline, "w") as _f:
            _yaml.safe_dump(cfg, _f, default_flow_style=False, sort_keys=False)


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
- `qc/trimmed/`: FastQC reports for trimmed reads.
- `qc/unmapped/`: FastQC reports for unmapped reads.
- `qc/rnaseq/`: coralsnake rnaseqc per-sample metrics + gene/exon count tables.
- `fastq/discarded/`: Reads discarded during trimming (adapter dimers, too-short, or low-quality).
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
- `stats/report/`: Metagene / logo / motif-enrichment profiles and the
  rendered HTML sections consumed by `final_report`.

### 5. `pileup/`
- `pileup/per_sample/`: Site-level data (tsv.gz) for each sample.
- `pileup/transcript.parquet`: Merged transcriptome pileup.
- `pileup/genome.parquet`: Merged genomic pileup.

---
*Note: For final results (including merged `sites.tsv.gz`), see `report_reads/` and `report_sites/`.*
EOF
        """


# Prepare a reference FASTA for a layer.  Two cases:
#   * user-supplied FASTA (list) -> combine into internal_files/ref/<key>.fa
#   * GTF-derived (gtf + liftover) -> coralsnake prepare -> <key>.fa + <key>.tsv
# The reference's ``liftover`` target (e.g. genome) supplies the genome FASTA
# used by prepare to extract transcript sequences.
def _ref_is_gtf(key: str) -> bool:
    """True if the reference is GTF-derived (declares a liftover target).

    A reference with ``liftover`` is a feature reference (e.g. mrna) built from
    a GTF; the genome reference has a ``gtf`` for annotation but is NOT
    GTF-derived (no liftover).
    """
    ref = REF.get(key, {})
    return bool(ref.get("liftover"))

# Prepare a reference FASTA for a layer.  Two cases:
#   * GTF-derived (gtf + liftover) -> coralsnake prepare from the target's
#     genome FASTA + the GTF, producing <key>.fa + <key>.tsv.
#   * user-supplied FASTA (list) -> combine into <key>.fa.
def _prepare_ref_inputs(key):
    ref = REF.get(key, {})
    if _ref_is_gtf(key):
        # GTF-derived: genome FASTA comes from the liftover target (e.g. genome).
        target = ref.get("liftover")
        genome_fa = REF[target]["fa"] if target else None
        return {"fa": genome_fa or [], "gtf": ref["gtf"]}
    fa = ref.get("fa")
    return {"fa": fa or [], "gtf": []}

rule prepare_reference:
    input:
        fa=lambda wildcards: _prepare_ref_inputs(wildcards.key)["fa"],
        gtf=lambda wildcards: _prepare_ref_inputs(wildcards.key)["gtf"],
    output:
        fa=INTERNALDIR / "ref/{key}.fa",
        fai=INTERNALDIR / "ref/{key}.fa.fai",
        # <key>.tsv only for GTF-derived references (used by liftover).
        info=INTERNALDIR / "ref/{key}.tsv",
    threads: 16
    resources:
        mem_mb=64000
    benchmark:
        BENCHDIR / "prepare_reference_{key}.benchmark.txt"
    run:
        import os
        outdir = os.path.dirname(str(output.fa))
        os.makedirs(outdir, exist_ok=True)
        if input.gtf:
            shell(
                "{PATH.coralsnake} prepare -g {input.gtf} -f {input.fa} "
                "-o {output.info} -s {output.fa} -c -n -x -t -z"
            )
            shell("{PATH.samtools} faidx {output.fa} --fai-idx {output.fai}")
        else:
            shell("cat {input.fa} > {output.fa}")
            shell("{PATH.samtools} faidx {output.fa} --fai-idx {output.fai}")


# ---------------------------------------------------------------------------
# Phase 2: Trimming & read QC
# ---------------------------------------------------------------------------


rule trim_se:
    input:
        lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R1") or [],
    output:
        c=temp(TEMPDIR / "trim/SE/{sample}_{rn}_R1.fq.gz"),
        s=temp(TEMPDIR / "trim/SE/{sample}_{rn}_discarded_R1.fq.gz"),
        report=temp(TEMPDIR / "trim/SE/{sample}_{rn}_mqc.tsv"),
    params:
        minlen=config.get("min_len", 20),
        trim=str(config.get("trim", True)).lower(),
        cut=lambda wildcards: f"-A '{SAMPLE2ADAPTER[wildcards.sample]}'",
    threads: 8
    benchmark:
        BENCHDIR / "trim_se_{sample}_{rn}.benchmark.txt"
    shell:
        """
        if [ "{params.trim}" = "false" ]; then
            cp {input} {output.c} && \
            gzip -n -c /dev/null > {output.s} && \
            printf 'sample\trun\tinput_reads\toutput_reads\tdiscarded_reads\tadapter\n{wildcards.sample}\t{wildcards.rn}\t0\t0\t0\tpassthrough\n' > {output.report}
        else
            {PATH.cutseq} -t {threads} {params.cut} -m {params.minlen} --auto-rc -o {output.c} -d {output.s} --json-file {output.report} {input}
        fi
        """


rule trim_pe:
    input:
        r1=lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R1") or [],
        r2=lambda wildcards: SAMPLE2DATA[wildcards.sample][wildcards.rn].get("R2") or [],
    output:
        c1=temp(TEMPDIR / "trim/PE/{sample}_{rn}_R1.fq.gz"),
        c2=temp(TEMPDIR / "trim/PE/{sample}_{rn}_R2.fq.gz"),
        s1=temp(TEMPDIR / "trim/PE/{sample}_{rn}_discarded_R1.fq.gz"),
        s2=temp(TEMPDIR / "trim/PE/{sample}_{rn}_discarded_R2.fq.gz"),
        report=temp(TEMPDIR / "trim/PE/{sample}_{rn}_mqc.tsv"),
    params:
        minlen=config.get("min_len", 20),
        trim=str(config.get("trim", True)).lower(),
        cut=lambda wildcards: f"-A '{SAMPLE2ADAPTER[wildcards.sample]}'",
    threads: 8
    benchmark:
        BENCHDIR / "trim_pe_{sample}_{rn}.benchmark.txt"
    shell:
        """
        if [ "{params.trim}" = "false" ]; then
            # trim: false -> passthrough (reads already trimmed/UMI-extracted upstream)
            cp {input.r1} {output.c1} && \
            cp {input.r2} {output.c2} && \
            gzip -n -c /dev/null > {output.s1} && \
            gzip -n -c /dev/null > {output.s2} && \
            printf 'sample\trun\tinput_reads\toutput_reads\tdiscarded_reads\tadapter\n{wildcards.sample}\t{wildcards.rn}\t0\t0\t0\tpassthrough\n' > {output.report}
        else
            {PATH.cutseq} -t {threads} {params.cut} -m {params.minlen} --auto-rc -o {output.c1} {output.c2} -d {output.s1} {output.s2} --json-file {output.report} {input.r1} {input.r2}
        fi
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
            / f"trim/PE/{wildcards.sample}_{wildcards.rn}_discarded_{wildcards.rd}.fq.gz"
            if is_pe(wildcards.sample, wildcards.rn)
            else TEMPDIR
            / f"trim/SE/{wildcards.sample}_{wildcards.rn}_discarded_{wildcards.rd}.fq.gz"
        ),
    output:
        INTERNALDIR / "fastq/discarded/{sample}_{rn}_{rd}.fq.gz",
    benchmark:
        BENCHDIR / "finalize_discarded_reads_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        "cp {input} {output}"


# ---------------------------------------------------------------------------
# Phase 2: Trimming & read QC (continued)
# ---------------------------------------------------------------------------
# 2b. QC of trimmed reads
# ---------------------------------------------------------------------------


rule qc_trimmed:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"trim/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}_{wildcards.rd}.fq.gz"
        ),
    output:
        html=INTERNALDIR / "qc/trimmed/{sample}_{rn}_{rd}/fastqc_report.html",
        text=INTERNALDIR / "qc/trimmed/{sample}_{rn}_{rd}/fastqc_data.txt",
        summary=INTERNALDIR / "qc/trimmed/{sample}_{rn}_{rd}/summary.txt",
    params:
        # falco >= 2.0 creates a subdir named after the input basename inside -o,
        # so point -o at the parent dir (falco makes the {sample}_{rn}_{rd} dir).
        lambda wildcards: INTERNALDIR / "qc/trimmed",
    benchmark:
        BENCHDIR / "qc_trimmed_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        "{PATH.falco} -o {params} {input}"


rule report_qc_trimmed:
    input:
        [
            INTERNALDIR / f"qc/trimmed/{sample}_{rn}_{rd}/fastqc_data.txt"
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


# ---------------------------------------------------------------------------
# 3a. Premap: contamination removal
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 3a'. Competitive mapping cascade (single prismalign pipeline command)
#
# Replaces the old premap_align_* + mainmap_align_* + remap_align_* rules with
# one ``prismalign -p pipeline_m6A.yaml`` invocation. The pipeline config
# describes the layered logic (contamination -> genes -> transcript -> genome);
# references, reads and outputs are bound here via each layer's ``key``. It
# emits one BAM per layer (contamination/genes/transcript/genome) plus the
# final unmapped reads, so downstream combine_bams / stats / count_reads keep
# working unchanged.
# ---------------------------------------------------------------------------

rule map_cascade:
    input:
        pipeline=PIPELINE_PATH,
        fq1=lambda wildcards: (
            TEMPDIR / f"trim/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}_R1.fq.gz"
        ),
        fq2=lambda wildcards: (
            TEMPDIR / f"trim/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}_R2.fq.gz"
            if is_pe(wildcards.sample, wildcards.rn)
            else []
        ),
        # Reference FASTA for each active layer (from the mapping block).
        refs=lambda wildcards: [REF_BY_LAYER[k] for k in LAYER_KEYS],
    output:
        contam=temp(TEMPDIR / "map/{libmode}/{sample}_{rn}.contam.bam") if has_layer("contamination") else [],
        genes=temp(TEMPDIR / "map/{libmode}/{sample}_{rn}.genes.bam") if has_layer("genes") else [],
        tx=temp(TEMPDIR / "map/{libmode}/{sample}_{rn}.transcript.bam") if has_layer("transcript") else [],
        genome=temp(TEMPDIR / "map/{libmode}/{sample}_{rn}.genome.bam"),
        unmap=temp(TEMPDIR / "map/{libmode}/{sample}_{rn}.final_unmap.fq"),
        summary=temp(TEMPDIR / "map/{libmode}/{sample}_{rn}.summary"),
    threads: 128
    benchmark:
        BENCHDIR / "map_cascade_{libmode}_{sample}_{rn}.benchmark.txt"
    params:
        # The prismalign pipeline YAML (declares which layers run).
        pipeline=PIPELINE_PATH,
        max_mismatches=MAX_MISMATCHES,
        # Build the -r key=path bindings from the active layers.  The genome
        # hisat-3n index prefix is a directory-style prefix (not a single file)
        # so it is appended after the FASTA; the contamination hisat-3n index
        # is likewise referenced by prefix.
        ref_bindings=lambda wildcards, input: " ".join(
            f"-r {k}={_layer_ref_binding(k)}" for k in LAYER_KEYS
        ),
        out_bindings=lambda wildcards, output: " ".join(
            f"-o {k}={getattr(output, _out_attr(k))}" for k in LAYER_KEYS
        ),
        r2=lambda wildcards, input: (
            f"-2 {input.fq2}" if is_pe(wildcards.sample, wildcards.rn) else ""
        ),
    shell:
        """
        set -eo pipefail
        {PATH.prismalign} -p {params.pipeline} \
            {params.ref_bindings} \
            -1 {input.fq1} {params.r2} \
            {params.out_bindings} \
            -u {output.unmap} -R {output.summary} \
            -t {threads} -m {params.max_mismatches}
        """


# Map each active layer's BAM to a per-run file in internal_files/bam/per_run/.
# The layer key -> output filename mapping is derived from the mapping block
# (contamination -> contamination, genes -> genes, transcript -> transcript,
# genome -> genome).  prismalign outputs unsorted BAMs, so we sort here.
rule finalize_map_bam:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"map/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.{_bam_suffix(wildcards.reftype)}.bam"
        ),
    output:
        INTERNALDIR / "bam/per_run/{sample}_{rn}.{reftype}.bam",
    threads: 64
    priority: 4
    benchmark:
        BENCHDIR / "finalize_map_bam_{sample}_{rn}_{reftype}.benchmark.txt"
    shell:
        "{PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output} {input}"


rule finalize_map_summary:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"map/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.summary"
        ),
    output:
        INTERNALDIR / "stats/map/{sample}_{rn}.summary",
    benchmark:
        BENCHDIR / "finalize_map_summary_{sample}_{rn}.benchmark.txt"
    shell:
        "cp {input} {output}"


rule finalize_unmapped_fq:
    input:
        lambda wildcards: (
            TEMPDIR
            / f"map/{get_lib_subdir(wildcards.sample, wildcards.rn)}/{wildcards.sample}_{wildcards.rn}.final_unmap.fq"
        ),
    output:
        INTERNALDIR / "fastq/unmapped/{sample}_{rn}_{rd}.fq.gz",
    params:
        # PE: each mate is written by its own job (R1 -> -1, R2 -> -2).  SE
        # reads carry flag 4 (unmapped) but NOT the 0x40/0x80 mate flags, so
        # ``samtools fastq -1``/``-o`` (which only write READ1/READ2) extract
        # nothing; SE reads are classified as READ_OTHER and go to ``-0``.
        # Both read the same unmapped BAM (prismalign -u).
        mate=lambda wildcards: (
            "-0"
            if not is_pe(wildcards.sample, wildcards.rn)
            else ("-1" if wildcards.rd == "R1" else "-2")
        ),
        # PE: the other mate stream is discarded; SE has no other stream.
        other=lambda wildcards: "-0 /dev/null" if is_pe(wildcards.sample, wildcards.rn) else "",
    benchmark:
        BENCHDIR / "finalize_unmapped_fq_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        """
        # prismalign writes the final unmapped reads as an interleaved BAM;
        # split into R1/R2 FASTQ (PE) or a single R1 stream (SE).  For PE the
        # mate flag is -1/-2 and the other mate goes to /dev/null; for SE the
        # mate flag is -0 (READ_OTHER) and there is no other stream.
        {PATH.samtools} fastq -F 0x900 {params.mate} {output} {params.other} -s /dev/null -n {input}
        """


rule qc_unmapped:
    input:
        INTERNALDIR / "fastq/unmapped/{sample}_{rn}_{rd}.fq.gz",
    output:
        html=INTERNALDIR / "qc/unmapped/{sample}_{rn}_{rd}/fastqc_report.html",
        text=INTERNALDIR / "qc/unmapped/{sample}_{rn}_{rd}/fastqc_data.txt",
        summary=INTERNALDIR / "qc/unmapped/{sample}_{rn}_{rd}/summary.txt",
    params:
        # falco >= 2.0 creates a subdir named after the input basename inside -o,
        # so point -o at the parent dir (falco makes the {sample}_{rn}_{rd} dir).
        lambda wildcards: INTERNALDIR / "qc/unmapped",
    benchmark:
        BENCHDIR / "qc_unmapped_{sample}_{rn}_{rd}.benchmark.txt"
    shell:
        """
        # falco cannot process an empty FASTQ (errors with \"invalid reads file
        # format\"); if there are no unmapped reads, emit empty outputs instead.
        if [ $(zcat {input} 2>/dev/null | wc -l) -eq 0 ]; then
            mkdir -p $(dirname {output.html})
            touch {output.html} {output.text} {output.summary}
        else
            {PATH.falco} -o {params} {input}
        fi
        """


rule unmapped_report:
    input:
        [
            INTERNALDIR / f"qc/unmapped/{s}_{r}_{i}/fastqc_data.txt"
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


# ---------------------------------------------------------------------------
# Phase 4: BAM merge + dedup + stats
# ---------------------------------------------------------------------------


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


rule rnaseq_qc:
    """Run coralsnake qc (rnaseqc-style) on the deduplicated genome BAM.

    Produces per-sample RNA-seq QC metrics + gene/exon count tables from the
    genome-aligned BAM + GTF.  The default mapping-quality (255) rejects all
    short-read alignments (MAPQ 16-60), so we lower it to a realistic 20.
    Single-end libraries use --unpaired (no proper-pair requirement).
    """
    input:
        bam=INTERNALDIR / "bam/{sample}.genome.bam",
        gtf=REF["genome"]["gtf"],
    output:
        metrics=INTERNALDIR / "qc/rnaseq/{sample}.metrics.tsv",
        genes=INTERNALDIR / "qc/rnaseq/{sample}.gene_reads.tsv",
        tpm=INTERNALDIR / "qc/rnaseq/{sample}.gene_tpm.tsv",
        exons=INTERNALDIR / "qc/rnaseq/{sample}.exon_reads.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "rnaseq_qc_{sample}.benchmark.txt"
    run:
        # coralsnake qc writes into --outdir; run it there and move outputs.
        import shutil
        outdir = INTERNALDIR / "qc/rnaseq"
        outdir.mkdir(parents=True, exist_ok=True)
        # PE if any run of the sample is paired-end.
        is_pe_sample = any(is_pe(sample, rn) for rn in SAMPLE2DATA[sample])
        unpaired = "" if is_pe_sample else "--unpaired"
        shell(
            "{PATH.coralsnake} qc --bam {input.bam} --gtf {input.gtf} "
            "--outdir {outdir} --sample {sample} {unpaired} "
            "--mapping-quality 20"
        )
        shutil.move(outdir / f"{sample}.metrics.tsv", output.metrics)
        shutil.move(outdir / f"{sample}.gene_reads.tsv", output.genes)
        shutil.move(outdir / f"{sample}.gene_tpm.tsv", output.tpm)
        shutil.move(outdir / f"{sample}.exon_reads.tsv", output.exons)


rule liftover_sites:
    input:
        transcripts=INTERNALDIR / "bam/{sample}.transcript.bam" if has_layer("transcript") else [],
        genome=INTERNALDIR / "bam/{sample}.genome.bam",
        info=INTERNALDIR / "ref/transcript.tsv" if has_layer("transcript") else [],
    output:
        transcripts=temp(TEMPDIR / "liftover/{sample}.transcript.bam") if has_layer("transcript") else [],
        bam=INTERNALDIR / "liftover_bam/{sample}.bam",
    params:
        fai=REF["genome"]["fa"] + ".fai",
    threads: 8
    benchmark:
        BENCHDIR / "liftover_sites_{sample}.benchmark.txt"
    shell:
        """
        if [ -n '{input.transcripts}' ] && [ -s '{input.transcripts}' ]; then
            {PATH.coralsnake} liftover -t {threads} -i {input.transcripts} -o {output.transcripts} -a {input.info} -f {params.fai}
            {PATH.samtools} cat {output.transcripts} {input.genome} | {PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output.bam}
        else
            # Genome-only run: no transcript layer to liftover; just copy the
            # genome BAM through (keeps the downstream liftover_bam contract).
            {PATH.samtools} sort -@ {threads} -m 3G -O BAM -o {output.bam} {input.genome}
        fi
        """


rule count_reads:
    input:
        report=lambda wildcards: [
            INTERNALDIR / f"qc/trimming/{wildcards.sample}_{r}_mqc.tsv"
            for r in SAMPLE2DATA[wildcards.sample].keys()
        ],
        count1=(
            INTERNALDIR / "stats/combined/{sample}.contamination.count"
            if has_layer("contamination")
            else []
        ),
        count2=(
            INTERNALDIR / "stats/dedup/{sample}.contamination.count"
            if has_layer("contamination")
            else []
        ),
        count3=(
            INTERNALDIR / "stats/combined/{sample}.genes.count" if has_layer("genes") else []
        ),
        count4=(INTERNALDIR / "stats/dedup/{sample}.genes.count" if has_layer("genes") else []),
        count5=(
            INTERNALDIR / "stats/combined/{sample}.transcript.count"
            if has_layer("transcript")
            else []
        ),
        count6=(
            INTERNALDIR / "stats/dedup/{sample}.transcript.count"
            if has_layer("transcript")
            else []
        ),
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


# ---------------------------------------------------------------------------
# Phase 5: Site calling & table merge/remap
# ---------------------------------------------------------------------------


rule spike_ratio:
    input:
        bam=lambda wildcards: (
            expand(INTERNALDIR / "bam/{sample}.genes.bam", sample=SAMPLE2DATA.keys())
            if has_layer("genes")
            else []
        ),
        bai=lambda wildcards: (
            expand(
                INTERNALDIR / "bam/{sample}.genes.bam.bai",
                sample=SAMPLE2DATA.keys(),
            )
            if has_layer("genes")
            else []
        ),
    output:
        tsv=INTERNALDIR / "stats/ratio/probe.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "spike_ratio.benchmark.txt"
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
    downstream consumers (unfilter_genes_stat / motif_rate /
    merge_samples) keep computing on u1/m1 only; u0/m0 are extra columns in
    the TSV.  NOTE: the 0.0.8 conversion gate reads the Yf/Zf
    (forward-channel) tags even for the C->T view -- kept here for parity.
    The 31-mer motif (pad 15) must stay in sync with substr($4,15,3) in
    motif_rate.
    """
    input:
        bam=INTERNALDIR / "bam/{sample}.{reftype}.bam",
        bai=INTERNALDIR / "bam/{sample}.{reftype}.bam.bai",
        ref=lambda wildcards: REF_BY_LAYER[wildcards.reftype],
    output:
        INTERNALDIR / "pileup/per_sample/{sample}.{reftype}.tsv.gz",
    params:
        # 2-group router (countmut >= 0.2.2): group 1 = high-conversion bases
        # (legacy 0.0.8 gate), group 0 = all other kept bases; NS > max_sub
        # drops the read (nil).  With equal trims the legacy per-strand
        # condition reduces to `qpos >= trim and qlen - qpos > trim`.
        router=lambda wildcards: (
            "([NS] <= {}) and (([Yf] >= {} and [Zf] <= {} and bq >= {}"
            " and qpos >= {} and qlen - qpos > {}) and 1 or 0)"
        ).format(
            COUNTMUT_MAX_SUB,
            COUNTMUT_MIN_CON,
            COUNTMUT_MAX_UNC,
            COUNTMUT_MIN_BASEQ,
            COUNTMUT_TRIM,
            COUNTMUT_TRIM,
        ),
        # Target-base sites only.  `base` is a strand-aware reference base:
        # it is the target base when the site is a mutation site on EITHER
        # strand (genomic ref = target = '+' strand site; genomic ref =
        # complement = '-' strand site).  countmut >= 0.2.5 evaluates -p PER
        # STRAND, so `base == 'A'` keeps only the A-site strand and drops the
        # spurious complement-strand rows automatically (no --target-base
        # needed).  All downstream consumers guard on u1+m1>0.
        site_filter=lambda wildcards: (
            "base == 'C' and (c + t) > 0"
            if config.get("pileup_ct", False)
            else "base == 'A' and (a + g) > 0"
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


rule motif_rate:
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
        target_base=BASE_CHANGE.split(",")[0].upper(),
    benchmark:
        BENCHDIR / "motif_rate_{sample}_{reftype}.benchmark.txt"
    shell:
        "zcat {input.pileup} | awk -F '\\t' -v target=\"{params.target_base}\" "
        '\'BEGIN{{OFS="\\t";print "Motif","Count","Unconverted","Depth","Ratio","Count_all","Unconverted_all","Depth_all","Ratio_all"}} '
        "NR>1 && ($5+$6+$7+$8+0)>0{{ "
        "m=toupper(substr($4,15,3)); "
        'if(m ~ "^[ATGC]+$" && substr(m,2,1) == target){{ '
        "a=$5+$6;g=$7+$8;d=a+g;na[m]++;ua[m]+=a;da[m]+=d;ra[m]+=a/d;"
        "if(($6+$8+0)>0){{n1[m]++;u1[m]+=$6;d1[m]+=$6+$8;r1[m]+=$6/($6+$8)}}}}}}"
        "END{{for(m in da) print m,(n1[m]+0),(u1[m]+0),(d1[m]+0),(n1[m]>0?r1[m]/n1[m]:0),na[m],ua[m],da[m],ra[m]/na[m]}}' > {output}"


rule join_pileup:
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
        BENCHDIR / "join_pileup_{reftype}.benchmark.txt"
    shell:
        """
        {PATH.merge_samples} --files {input} --names {params.samples} --output {output} --requires {params.requires}
        """


rule merge_sites:
    """Merge the site-calling pileups into the final sites table.

    With a transcript layer present, remap_genome lifts transcript sites to
    genome coords (strand-aware) and unions with genome sites.  For a
    genome-only run there is no transcript layer: the genome pileup is passed
    straight through (empty gene annotation), so the sites table keeps the
    same Chrom/Pos/Strand/GeneName/GenePos/Motif schema.
    """
    input:
        info=INTERNALDIR / "ref/transcript.tsv" if has_layer("transcript") else [],
        transcripts=(
            INTERNALDIR / "pileup/transcript.parquet" if has_layer("transcript") else []
        ),
        genome=INTERNALDIR / "pileup/genome.parquet",
    output:
        "report_sites/sites.tsv.gz",
    threads: 32
    benchmark:
        BENCHDIR / "merge_sites.benchmark.txt"
    resources:
        runtime=720
    params:
        # No transcript layer -> omit -t/-a; remap_genome passes the genome
        # pileup straight through (empty gene annotation).
        tx_args="" if not has_layer("transcript") else "-t {input.info} -a {input.transcripts}",
    shell:
        """
        {PATH.remap_genome} {params.tx_args} -b {input.genome} -o {output} --min-depth {config[min_merged_depth]}
        """


rule filter_sites:
    """Filter the raw sites table with a GC-background statistical model.

    Runs for every pipeline (not just eTAM): fits the per-motif m6A level vs
    GC background, computes a chi-square p-value per site, and keeps sites
    with a significant conversion (p < 1).  This is a general m6A site filter
    (the historical eTAM filter, made the default).
    """
    input:
        "report_sites/sites.tsv.gz",
    output:
        fl="report_sites/filtered.tsv",
    threads: 64
    benchmark:
        BENCHDIR / "filter_sites.benchmark.txt"
    shell:
        """
        {PATH.filter_sites} -i {input} -o {output.fl}
        """


rule annotate_sites:
    """Annotate the filtered sites table with gene/transcript/region info.

    Runs coralsnake annotate on report_sites/filtered.tsv (columns 1,2,3 =
    Chrom,Pos,Strand) against the genome GTF, appending gene_id,
    transcript_id, transcript_pos, region, gene_pos, etc.  Produces
    report_sites/filtered.annotated.tsv.
    """
    input:
        sites="report_sites/filtered.tsv",
        gtf=REF["genome"]["gtf"],
    output:
        "report_sites/filtered.annotated.tsv",
    threads: 8
    benchmark:
        BENCHDIR / "annotate_sites.benchmark.txt"
    shell:
        """
        {PATH.coralsnake} annotate -i {input.sites} -o {output} \
            --reference-gtf {input.gtf} -c 1,2,3 -H -s
        """


rule group_sites:
    input:
        "report_sites/sites.tsv.gz",
    output:
        "report_sites/grouped/{group}.parquet",
    params:
        names=lambda wildcards: GROUP2SAMPLE[wildcards.group],
    threads: 8
    benchmark:
        BENCHDIR / "group_sites_{group}.benchmark.txt"
    shell:
        """
        {PATH.sum_groups} -i {input} -o {output} -n {params.names}
        """


# ---------------------------------------------------------------------------
# Phase 6: Report generation
# ---------------------------------------------------------------------------


rule mqc_mapping:
    input:
        counts=expand(
            INTERNALDIR / "stats/count/{sample}.tsv", sample=SAMPLE2DATA.keys()
        ),
        dedup_logs=expand(
            INTERNALDIR / "stats/dedup/{sample}.{reftype}.log",
            sample=SAMPLE2DATA.keys(),
            reftype=REFTYPES,
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
        BENCHDIR / "mqc_mapping.benchmark.txt"
    threads: 4
    shell:
        """
        mkdir -p $(dirname {output.mapping})
        {PATH.mqc_mapping} {output.mapping} {output.dedup} {input.counts} --dedup-logs {input.dedup_logs} --trim-jsons {input.trim_jsons}
        """


rule mqc_sites:
    input:
        motifs=expand(
            INTERNALDIR / "stats/ratio/by_motif/{sample}.{reftype}.tsv",
            sample=SAMPLE2DATA.keys(),
            reftype=SITE_REFTYPES,
        ),
        sites_file=[
            INTERNALDIR / f"pileup/{r}.parquet" for r in SITE_REFTYPES
        ],
    output:
        motifs=INTERNALDIR / "stats/mqc/sites/motif_conversion_mqc.tsv",
        site_sum=INTERNALDIR / "stats/mqc/sites/site_summary_mqc.tsv",
        site_dist=INTERNALDIR / "stats/mqc/sites/site_distribution_mqc.tsv",
        site_depth=INTERNALDIR / "stats/mqc/sites/site_depth_mqc.tsv",
        reftype_tables=expand(
            INTERNALDIR / "stats/mqc/sites/motif_ratio_{reftype}_mqc.tsv",
            reftype=SITE_REFTYPES,
        ),
    params:
        target_base=BASE_CHANGE.split(",")[0],
        reftype_tables=lambda wildcards, output: " ".join(
            f"--reftype-table {r}={output.reftype_tables[i]}"
            for i, r in enumerate(SITE_REFTYPES)
        ),
    benchmark:
        BENCHDIR / "mqc_sites.benchmark.txt"
    threads: 16
    resources:
        runtime=720
    shell:
        """
        mkdir -p $(dirname {output.motifs})
        {PATH.mqc_sites} {output.motifs} {output.site_sum} {output.site_dist} {output.site_depth} {params.reftype_tables} --motif-files {input.motifs} --sites-file {input.sites_file} --target-base {params.target_base}
        """


rule report_mapping:
    input:
        INTERNALDIR / "stats/mqc/reads/mapping_stats_mqc.tsv",
        INTERNALDIR / "stats/mqc/reads/dedup_stats_mqc.tsv",
        # map_cascade produces ONE summary with all layer stats; the old
        # premap/mainmap/remap split is gone.
        expand(
            INTERNALDIR / "stats/map/{sample}_{rn}.summary",
            sample=SAMPLE2DATA.keys(),
            rn=["run1"],
        ),
    output:
        "report_reads/mapping.html",
    params:
        report_name="mapping.html",
        report_dir=str(Path("report_reads")),
    benchmark:
        BENCHDIR / "report_mapping.benchmark.txt"
    shell:
        "{PATH.report_html} tables {output} {input}"


rule report_sites:
    """Render all site-related report sections into one sites.html.

    Sections: the site table (from mqc stats), metagene coverage + sequence
    logo (from sites.tsv.gz), and per-sample motif conversion + enrichment
    (from the per-motif rate tables).  Each section is rendered to a temp HTML
    then assembled into report_sites/sites.html.
    """
    input:
        # site table stats (from mqc_sites)
        mqc=expand(
            INTERNALDIR / "stats/mqc/sites/{f}",
            f=["motif_conversion_mqc.tsv", "site_summary_mqc.tsv",
               "site_distribution_mqc.tsv", "site_depth_mqc.tsv"],
        ),
        motif_ratio=expand(
            INTERNALDIR / "stats/mqc/sites/motif_ratio_{reftype}_mqc.tsv",
            reftype=SITE_REFTYPES,
        ),
        # site context (metagene + logo)
        sites="report_sites/sites.tsv.gz",
        gtf=REF["genome"]["gtf"],
        # per-sample motif conversion + enrichment
        by_motif=expand(
            INTERNALDIR / "stats/ratio/by_motif/{sample}.{reftype}.tsv",
            sample=SAMPLE2DATA.keys(),
            reftype=SITE_REFTYPES,
        ),
        by_motif_genome=expand(
            INTERNALDIR / "stats/ratio/by_motif/{sample}.genome.tsv",
            sample=SAMPLE2DATA.keys(),
        ),
        filtered="report_sites/filtered.tsv",
    output:
        "report_sites/sites.html",
    threads: 8
    benchmark:
        BENCHDIR / "report_sites.benchmark.txt"
    run:
        import os, tempfile, subprocess, shutil
        tmpdir = tempfile.mkdtemp(prefix="report_sites_")
        try:
            def run(cmd):
                subprocess.run(cmd, shell=True, check=True)

            # 1. site table
            table_html = os.path.join(tmpdir, "table.html")
            run(f"{PATH.report_html} tables {table_html} "
                + " ".join(str(p) for p in input.mqc)
                + " " + " ".join(str(p) for p in input.motif_ratio))
            # 2. metagene (compute + render)
            prof = os.path.join(tmpdir, "metagene.tsv")
            run(f"{PATH.coralsnake} metagene -i {input.sites} -g {input.gtf} -H "
                f"--meta-columns 1,2,3 --bins 100 --export-profile {prof}")
            meta_html = os.path.join(tmpdir, "metagene.html")
            run(f"{PATH.report_html} metagene {meta_html} {prof}")
            # 3. logo (compute + render)
            logo_html = os.path.join(tmpdir, "logo.html")
            run(f"zcat {input.sites} | awk -F '\\t' 'NR==1{{for(i=7;i<=NF;i++) if($$i ~ /^Depth_/) d[i]=1; next}} "
                f"{{s=0; for(i in d) s+=$$i; if($$6 ~ /^[ACGTUNn]+$/ && s>0) print $$6 \"\\t\" s}}' "
                f"| {PATH.coralsnake} logo -i - --matrix {logo_html}")
            # 4. per-sample motif conversion + enrichment
            sections = [table_html, meta_html, logo_html]
            by_motif = [str(p) for p in input.by_motif]
            by_motif_genome = [str(p) for p in input.by_motif_genome]
            for si, sample in enumerate(SAMPLE2DATA):
                for reftype in SITE_REFTYPES:
                    motif_html = os.path.join(tmpdir, f"motif_{sample}_{reftype}.html")
                    run(f"{PATH.report_html} motifconv {motif_html} "
                        + " ".join(by_motif))
                    sections.append(motif_html)
                enrich_tsv = os.path.join(tmpdir, f"enrich_{sample}.tsv")
                run(f"{PATH.motif_enrich} -i {by_motif_genome[si]} -f {input.filtered} "
                    f"-s {sample} -o {enrich_tsv}")
                enrich_html = os.path.join(tmpdir, f"enrich_{sample}.html")
                run(f"{PATH.report_html} motiffig {enrich_html} {enrich_tsv} {sample}")
                sections.append(enrich_html)
            # 5. assemble
            os.makedirs(os.path.dirname(str(output)), exist_ok=True)
            run(f"{PATH.report_html} assemble {output} " + " ".join(sections))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


rule report_rnaseq:
    """Render the coralsnake rnaseq_qc metrics into a per-sample QC table.

    Reads internal_files/qc/rnaseq/{sample}.metrics.tsv (read length, fragment
    sizes, mapping/exonic rates, etc.) and pivots them into one table for the
    final report.
    """
    input:
        expand(
            INTERNALDIR / "qc/rnaseq/{sample}.metrics.tsv",
            sample=SAMPLE2DATA.keys(),
        ),
    output:
        INTERNALDIR / "stats/report/rnaseq.html",
    benchmark:
        BENCHDIR / "report_rnaseq.benchmark.txt"
    shell:
        "{PATH.report_html} rnaseq {output} {input}"


rule final_report:
    """Assemble one self-contained report.html from all per-section HTML."""
    input:
        # All report sections, referenced uniformly via ``rules.<rule>.output``.
        rules.report_qc_trimmed.output,
        rules.unmapped_report.output,
        rules.report_mapping.output,
        rules.report_sites.output,   # site table + metagene + logo + motif + enrich
        rules.report_rnaseq.output,
    output:
        "report.html",
    benchmark:
        BENCHDIR / "final_report.benchmark.txt"
    shell:
        """
        mkdir -p report_reads report_sites
        {PATH.report_html} assemble {output} {input}
        """
