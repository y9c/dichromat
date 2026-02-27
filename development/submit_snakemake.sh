#!/bin/bash
#SBATCH --job-name=dichromat_master
#SBATCH --partition=normal
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=24:00:00
#SBATCH --output=dichromat_master_%j.log

cd /data/project/changye/chye/projects/m6A_new

# Load modules if needed
source /data/mgt/modules-5.6.1/init/bash
module load apptainer/1.4.5

# Run snakemake entirely on compute node
snakemake \
    --configfile config.yaml \
    -s Snakefile \
    --use-apptainer \
    --apptainer-args "--bind /data:/data" \
    --executor slurm \
    --jobs 100 \
    --latency-wait 60 \
    --default-resources \
        slurm_partition=normal \
        slurm_account=lab-changye \
        runtime=2160 \
        mem_mb="4000*threads"
