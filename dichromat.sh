#!/bin/bash

logo="
\033[1;31m     ▄████▄   \033[1;37m▄████▄   \033[1;31m▄████▄   \033[1;37m▄████▄   \033[1;31m▄████▄
\033[1;31m    ██  ▀██▄ \033[1;37m██  ▀██▄ \033[1;31m██  ▀██▄ \033[1;37m██  ▀██▄ \033[1;31m██  ▀██▄  \033[1;37m
\033[1;31m    ▀██████▀ \033[1;37m ▀██████▀ \033[1;31m ▀██████▀ \033[1;37m ▀██████▀ \033[1;31m ▀██████▀

\033[1;37m    \033[38;2;255;0;0mD \033[38;2;255;32;0mI \033[38;2;255;64;0mC \033[38;2;255;96;0mH \033[38;2;255;128;0mR \033[38;2;255;160;0mO \033[38;2;255;192;0mM \033[38;2;255;224;0mA \033[38;2;255;255;0mT\033[0m
\033[1;34m  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m
\033[1;32m   General Pipeline for eTAM-seq, CAM-seq, GLORI, BS-seq, etc. \033[0m
\033[1;34m  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m
"

printf "$logo\n"

set -e

# Get project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load Apptainer module (required for cluster environment)
if [ -f /data/mgt/modules-5.6.1/init/bash ]; then
    source /data/mgt/modules-5.6.1/init/bash
    module load apptainer/1.4.5 || true
fi

# Check for batch argument
BATCH=""
EXTRA_ARGS=()

for ((i=1; i<=$#; i++)); do
    arg="${!i}"
    if [ "$arg" = "--batch" ]; then
        next=$((i+1))
        BATCH="${!next}"
        i=$((i+1))  # Skip the next argument
    elif [[ "$arg" == --batch=* ]]; then
        BATCH="${arg#--batch=}"
    else
        EXTRA_ARGS+=("$arg")
    fi
done

# Validate batch name
if [ -z "$BATCH" ]; then
    echo "Error: --batch is required"
    echo "Usage: ./dichromat.sh --batch <batch_name> [snakemake_args...]"
    echo ""
    echo "Examples:"
    echo "  ./dichromat.sh --batch mytest"
    exit 1
fi

# Create workspace directory
mkdir -p "${PROJECT_DIR}/workspace_${BATCH}"

# Set up log file
export LC_ALL=C.UTF-8
LOGFILE="${PROJECT_DIR}/workspace_${BATCH}/dichromat_LOG_$(date +"%F-%H%M%S").txt"

echo -e "\033[0;32mLog file:\033[0m ${LOGFILE}"
echo "Starting pipeline..."

# Run snakemake directly
cd "${PROJECT_DIR}"

if [ -d "${PROJECT_DIR}/development/.venv" ]; then
    SNAKEMAKE_BIN="${PROJECT_DIR}/development/.venv/bin/snakemake"
else
    SNAKEMAKE_BIN="snakemake"
fi

# Run snakemake - profile must be specified by user for SLURM
echo "Running pipeline... (output logged to: ${LOGFILE})"

"$SNAKEMAKE_BIN" \
    --configfile "${PROJECT_DIR}/config.yaml" \
    -p --rerun-incomplete \
    -s "${PROJECT_DIR}/Snakefile" \
    --directory "${PROJECT_DIR}/workspace_${BATCH}" \
    --config batch="$BATCH" \
    -j 100 \
    --use-singularity \
    --singularity-args "-B /data" \
    "${EXTRA_ARGS[@]}" >> "${LOGFILE}" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "\033[0;32m✓ Pipeline completed successfully\033[0m"
else
    echo -e "\033[0;31m✗ Pipeline failed with exit code $EXIT_CODE\033[0m"
fi

exit $EXIT_CODE
