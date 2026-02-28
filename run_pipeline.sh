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
BENCH=false

# Display usage
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -b, --batch BATCH    Batch name (default: $BATCH)"
    echo "  -c, --config CONFIG  Config file (default: $CONFIG)"
    echo "  -j, --jobs JOBS      Max parallel jobs (default: $JOBS)"
    echo "  -p, --profile PROF   Snakemake profile (default: $PROFILE)"
    echo "  --bench              Enable benchmarking for runtime/resource tracking"
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
        --bench)     BENCH=true; shift ;;
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
if [ "$BENCH" = true ]; then
    echo -e "\033[0;34mBenchmarking enabled\033[0m"
fi
echo -n "Analyzing... "

# Cleanup function for terminal state
cleanup_status() {
    local status=$1
    if [ "$status" -eq 0 ]; then
        echo -e "\r\033[K\033[0;32m✔\033[0m Successfully finished all jobs."
    else
        echo -e "\r\033[K\033[0;31m✗\033[0m Jobs exit with error!"
    fi
}

# Build snakemake command
SNAKEMAKE_CMD="snakemake --configfile $CONFIG \
          -p --rerun-incomplete \
          -s Snakefile \
          --directory ${PROJECT_DIR}/workspace_${BATCH} \
          --config batch=$BATCH \
          -j $JOBS \
          --profile $PROFILE \
          --use-apptainer \
          --apptainer-args \"-B /data -B ${PROJECT_DIR}\" \
          --latency-wait 60"

# Add benchmarking if enabled
MONITOR_PID=""
USE_LOGGER_PLUGIN=false
if [ "$BENCH" = true ]; then
    SNAKEMAKE_CMD="$SNAKEMAKE_CMD --benchmark-extended"
    
    # Check if logger plugin is available in uv environment
    if [ -d "${PROJECT_DIR}/development/.venv" ]; then
        if (cd "${PROJECT_DIR}/development" && uv run python -c "import snakemake_logger_resource" 2>/dev/null); then
            echo -e "\033[0;34mUsing Snakemake resource logger plugin\033[0m"
            SNAKEMAKE_CMD="$SNAKEMAKE_CMD --logger resource --logger-resource-interval 30"
            USE_LOGGER_PLUGIN=true
        fi
    fi
    
    # Fallback to monitor script if plugin not available
    if [ "$USE_LOGGER_PLUGIN" = false ]; then
        echo -e "\033[0;33mResource logger plugin not installed, using fallback monitor\033[0m"
        
        # Function to cleanup monitor
        cleanup_monitor() {
            if [ -n "$MONITOR_PID" ]; then
                echo -e "\n\033[0;34mStopping resource monitor...\033[0m" >&2
                kill -TERM $MONITOR_PID 2>/dev/null || true
                for i in 1 2 3 4 5; do
                    if ! kill -0 $MONITOR_PID 2>/dev/null; then
                        break
                    fi
                    sleep 1
                done
                kill -9 $MONITOR_PID 2>/dev/null || true
                wait $MONITOR_PID 2>/dev/null || true
            fi
        }
        
        # Set trap to cleanup monitor on exit
        trap cleanup_monitor EXIT INT TERM
        
        # Start resource monitor
        if [ -d "${PROJECT_DIR}/development/.venv" ]; then
            (
                cd "${PROJECT_DIR}/development" && uv run python monitor_resources.py "$USER" 30
            ) > "${LOGFILE}.resources" 2>&1 &
            MONITOR_PID=$!
        else
            python "${PROJECT_DIR}/development/monitor_resources.py" "$USER" 30 > "${LOGFILE}.resources" 2>&1 &
            MONITOR_PID=$!
        fi
    fi
fi

# Run Snakemake and capture output
eval $SNAKEMAKE_CMD "$@" > "${LOGFILE}" 2>&1

# Note: cleanup_monitor is called via trap on exit (only for fallback)

EXIT_CODE=$?
cleanup_status $EXIT_CODE

# Generate benchmark report if enabled and benchmarks exist
if [ "$BENCH" = true ]; then
    # Show real-time resource summary (only for fallback monitor)
    if [ "$USE_LOGGER_PLUGIN" = false ] && [ -f "${LOGFILE}.resources" ]; then
        # Check if file has content
        if [ -s "${LOGFILE}.resources" ]; then
            echo ""
            echo -e "\033[0;34mReal-time Resource Summary:\033[0m"
            tail -20 "${LOGFILE}.resources"
        else
            # Remove empty file
            rm -f "${LOGFILE}.resources"
        fi
    fi
    
    # Generate visual benchmark report
    BENCHMARK_DIR="${PROJECT_DIR}/workspace_${BATCH}/.snakemake/benchmarks"
    if [ -d "$BENCHMARK_DIR" ] && [ "$(ls -A $BENCHMARK_DIR/*.benchmark.txt 2>/dev/null)" ]; then
        echo ""
        REPORT_FILE="${PROJECT_DIR}/workspace_${BATCH}/benchmark_report_$(date +%Y%m%d_%H%M%S).txt"
        if [ -d "${PROJECT_DIR}/development/.venv" ]; then
            cd "${PROJECT_DIR}/development" && uv run python benchmark_report.py "$BENCHMARK_DIR" "$REPORT_FILE" 2>/dev/null || true
        else
            python "${PROJECT_DIR}/development/benchmark_report.py" "$BENCHMARK_DIR" "$REPORT_FILE" 2>/dev/null || true
        fi
    fi
fi

exit $EXIT_CODE
