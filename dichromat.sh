#!/bin/bash
# Simple dichromat pipeline runner without benchmarking

set -e

# Get project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
"$SNAKEMAKE_BIN" \
    --configfile "${PROJECT_DIR}/config.yaml" \
    -p --rerun-incomplete \
    -s "${PROJECT_DIR}/Snakefile" \
    --directory "${PROJECT_DIR}/workspace_${BATCH}" \
    --config batch="$BATCH" \
    -j 100 \
    "${EXTRA_ARGS[@]}" 2>&1 | tee -a "${LOGFILE}"

EXIT_CODE=${PIPESTATUS[0]}

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "\033[0;32m✓ Pipeline completed successfully\033[0m"
else
    echo -e "\033[0;31m✗ Pipeline failed with exit code $EXIT_CODE\033[0m"
fi

exit $EXIT_CODE
