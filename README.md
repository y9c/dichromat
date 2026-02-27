# dichromat

`dichromat` is an ultra-fast, containerized pipeline for m6A eTAM-seq data analysis. It handles everything from competitive mapping to site-calling and aggregated reporting.

## 📋 Requirements

To run this pipeline, you need the following installed on your host system:
*   **Snakemake** (>= 8.0 recommended)
*   **Apptainer** (or Singularity)
*   **Python 3** (with `pyyaml`)

## 🚀 Quick Start

### 1. Get the Container
The most straightforward way is to build the Apptainer image directly from GitHub Container Registry:
```bash
apptainer build dichromat.sif docker://ghcr.io/y9c/dichromat:latest
```

### 2. Configure Your Run
Edit `config.yaml` to specify your reference files and sample data. 

### 3. Run the Pipeline

There are two primary ways to execute the pipeline, depending on your environment:

#### Method A: Host-Controlled (Recommended for HPC)
In this mode, Snakemake runs on your host and manages the container execution for each rule.
*   **Pros**: Full flexibility for cluster integration (Slurm, LSF, etc.) and better resource management.
*   **Command**:
    ```bash
    ./run_pipeline.sh --batch your_batch_name
    ```

#### Method B: Container-Controlled (Zero Setup)
In this mode, you run the container directly, which internally executes Snakemake.
*   **Pros**: Portable and requires zero host-side configuration (except Apptainer).
*   **Command**:
    ```bash
    apptainer run -B /data:/data dichromat.sif -c config.yaml
    ```
    *(Note: Ensure you bind-mount your data folders using the `-B` flag as shown.)*


## 📊 Key Features
*   **Competitive Mapping**: Simultaneously align to transcriptome and masking references (rRNA, spike-ins).
*   **Fast Mutation Counting**: Uses `countmut` for ultra-fast, strand-aware processing.
*   **Unified Dashboards**: Generates MultiQC reports for mapping (`report_reads/mapping.html`) and site-calling (`report_sites/motif.html`).

## 📁 Output Structure
*   `report_reads/`: Mapping statistics and read-level QC dashboards.
*   `report_sites/`: Site-level analysis, filtered sites (`filtered.tsv`), and motif dashboards.
*   `internal_files/`: Intermediate alignments and statistical summaries.

---
For detailed build and publishing instructions, see [DEVELOP.md](DEVELOP.md).

Developed by **Ye Chang** (yech1990@gmail.com)
