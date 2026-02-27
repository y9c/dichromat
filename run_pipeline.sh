#!/usr/bin/env bash
#
# Host-side entrypoint for m6A dichromat Pipeline
# Executes Snakemake with Apptainer integration on the cluster.
#

# Default parameters
BATCH="dichromat_run"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="config.yaml"
JOBS=100
PROFILE="slurm_changye"

# Display usage
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -b, --batch BATCH    Batch name (default: $BATCH)"
    echo "  -c, --config CONFIG  Config file (default: $CONFIG)"
    echo "  -j, --jobs JOBS      Max parallel jobs (default: $JOBS)"
    echo "  -p, --profile PROF   Snakemake profile (default: $PROFILE)"
    echo "  -u, --unlock         Unlock the working directory"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Additional arguments will be passed directly to Snakemake."
    exit 1
}

UNLOCK=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--batch)  BATCH="$2"; shift 2 ;;
        -c|--config) CONFIG="$2"; shift 2 ;;
        -j|--jobs)   JOBS="$2"; shift 2 ;;
        -p|--profile) PROFILE="$2"; shift 2 ;;
        -u|--unlock) UNLOCK=true; shift ;;
        -h|--help)   usage ;;
        --)          shift; break ;;
        *)           break ;;
    esac
done

# Initialize Apptainer module
source /data/mgt/modules-5.6.1/init/bash && module load apptainer/1.4.5

if [ "$UNLOCK" = true ]; then
    echo "🔓 Unlocking pipeline for batch: $BATCH"
    snakemake --configfile "$CONFIG" \
              -s Snakefile \
              --directory "${PROJECT_DIR}/workspace_${BATCH}" \
              --config batch="$BATCH" \
              --unlock
    exit $?
fi

export LC_ALL=C.UTF-8
LOGFILE="dichromat_LOG_$(date +"%F-%H%M%S").txt"
echo -e "\033[0;32mREAD DEBUG LOG AT\033[0m ${LOGFILE}"
printf "\033[0;33m Analyzing...\033[0m"

# Run Snakemake and capture output
snakemake --configfile "$CONFIG" \
          -p --rerun-incomplete \
          -s Snakefile \
          --directory "${PROJECT_DIR}/workspace_${BATCH}" \
          --config batch="$BATCH" \
          -j "$JOBS" \
          --profile "$PROFILE" \
          --use-apptainer \
          --apptainer-args "-B /data -B ${PROJECT_DIR}" \
          --latency-wait 60 \
          "$@" > "${LOGFILE}" 2>&1

if [ $? -eq 0 ]; then
  printf "\033[0;33m\b\b\b\b\b\b\b\b\b\b\b\b\b\033[0m"
  printf "\033[0;32m\xE2\x9C\x94\033[0m Successfully finished all jobs.\n"
else
  printf "\033[0;33m\b\b\b\b\b\b\b\b\b\b\b\b\b\b\033[0m"
  printf "\033[0;31m\xE2\x9D\x8C\033[0m Jobs exit with error!\n"
fi
