# dichromat

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18803090.svg)](https://doi.org/10.5281/zenodo.18803090)

`dichromat` is a general, containerized pipeline for conversion sequencing of RNA modifications (e.g., eTAM-seq, CAM-seq, GLORI, BS-seq). It handles everything from competitive mapping to site-calling and aggregated reporting.

## 📋 Requirements

To run this pipeline, you need the following installed on your host system:
*   **Apptainer** (or Singularity)
*   **Snakemake** (>= 8.0 recommended, optional)

## 🚀 Quick Start

### 1. Get the Container

**Option A: Build from GitHub Container Registry (recommended):**
```bash
apptainer build dichromat.sif docker://ghcr.io/y9c/dichromat:latest
```

**Option B: Download pre-built SIF from Zenodo:**

<sub>*If you have networking issues accessing Docker Hub (especially in China), download the pre-built SIF directly:*</sub>
```bash
wget -O dichromat.sif "https://zenodo.org/api/records/18821558/draft/files/dichromat.sif/content"
```

### 2. Configure Your Run

Create a `config.yaml` with your references and samples (set `container` to your SIF path):

<details>
<summary>▼ Click to see the example configuration</summary>

```yaml
# Container image path
container: "./dichromat.sif"

# Reference files
reference:
  contamination:  # E.coli, etc. (optional)
    - ~/ref/contamination.fa
  genes:          # rRNA, tRNA, spike-ins for masking (optional)
    - ~/ref/rRNA_tRNA.fa
  genome:
    fa: ~/ref/GRCh38.fa
    gtf: ~/ref/GRCh38.gtf
    hisat3n: ~/ref/GRCh38_hisat3n_index

# Samples: paired-end (R1+R2) or single-end (R1 only)
samples:
  sample1:
    data:
      - R1: ~/data/sample1_R1.fq.gz
        R2: ~/data/sample1_R2.fq.gz

# Options
is_etam: true              # eTAM analysis mode
adapter: "AGATCGGA..."     # Custom adapter OR use libtype: "TAKARAV3"
base_change: "A,G"         # m6A/GLORI: A,G  |  BS-seq: C,T
```

</details>

See `default.yaml` for all available options. 

### 3. Run the Pipeline

There are two primary ways to execute the pipeline, depending on your environment:

#### Method A: Host-Controlled (Recommended for HPC)
In this mode, Snakemake runs on your host and manages the container execution for each rule.
*   **Pros**: Full flexibility for cluster integration (Slurm, LSF, etc.) and better resource management.
*   **Command**:
    ```bash
    ./dichromat.sh --batch your_batch_name
    ```

#### Method B: Container-Controlled (Zero Setup)
In this mode, you run the container directly, which internally executes Snakemake.
*   **Pros**: Portable and requires zero host-side configuration (except Apptainer).
*   **Command**:
    ```bash
    apptainer run -B /data dichromat.sif -c config.yaml
    ```
    *(Note: Ensure you bind-mount your data folders using the `-B` flag as shown.)*

## 📊 Key Features
*   **Competitive Mapping**: Simultaneously align to transcriptome and masking references (rRNA, spike-ins).
*   **Fast Mutation Counting**: Uses `countmut` for ultra-fast, strand-aware processing.

## 📁 Output Structure
*   `report_reads/`: Mapping statistics and read-level QC dashboards.
*   `report_sites/`: Site-level analysis, filtered sites (`filtered.tsv`), and motif dashboards.
*   `internal_files/`: Intermediate alignments and statistical summaries.

---

For detailed build and publishing instructions, see [DEVELOP.md](DEVELOP.md).

Developed by **Ye Chang** (yech1990@gmail.com)
