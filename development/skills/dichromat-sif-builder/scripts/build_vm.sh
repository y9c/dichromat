#!/bin/bash
#
# Build dichromat.sif using a temporary VM with Docker support
# Usage: build_vm.sh [sif_name]
#

set -e

SIF_NAME="${1:-${SIF_NAME:-dichromat.sif}}"
VM_SCRIPT="${VM_SCRIPT:-/data/share/vm/submit_vm.sh}"
SSH_KEY="${SSH_KEY:-${HOME}/.ssh/id_vm_rsa}"
VM_ACCOUNT="${VM_ACCOUNT:-lab-changye}"
VM_CPUS="${VM_CPUS:-128}"
VM_MEM="${VM_MEM:-400G}"
BUILD_DIR="pipeline_build"

PROJECT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$PROJECT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Starting VM-based build process...${NC}"
echo -e "${BLUE}   Project: $PROJECT_DIR${NC}"
echo -e "${BLUE}   Output: $SIF_NAME${NC}"

# Step 1: Submit VM job
echo -e "${YELLOW}📤 Submitting VM job to SLURM...${NC}"
JOB_OUTPUT=$(sbatch --account="$VM_ACCOUNT" --cpus-per-task="$VM_CPUS" --mem="$VM_MEM" "${VM_SCRIPT}" 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to submit VM job:${NC}"
    echo "$JOB_OUTPUT"
    exit 1
fi

JOB_ID=$(echo "$JOB_OUTPUT" | grep -oP 'Submitted batch job \K\d+')
echo -e "${GREEN}✅ VM job submitted: $JOB_ID${NC}"

# Cleanup function
cleanup() {
    if [ -n "$JOB_ID" ]; then
        echo -e "${YELLOW}🧹 Cleaning up VM job $JOB_ID...${NC}"
        scancel "$JOB_ID" 2>/dev/null || true
        rm -f "slurm-${JOB_ID}.out"
    fi
}
trap cleanup EXIT

# Step 2: Wait for VM to be ready
echo -e "${YELLOW}⏳ Waiting for VM to start (this may take 2-3 minutes)...${NC}"
LOG_FILE="slurm-${JOB_ID}.out"
TIMEOUT=300  # 5 minutes
START_TIME=$(date +%s)

while true; do
    if [ -f "$LOG_FILE" ]; then
        PORT=$(grep -oP 'Using port: \K\d+' "$LOG_FILE" 2>/dev/null)
        NODE=$(grep -oP 'Node\s+: \K\w+' "$LOG_FILE" 2>/dev/null)
        
        if [ -n "$PORT" ] && [ -n "$NODE" ]; then
            echo ""
            echo -e "${GREEN}✅ VM ready!${NC}"
            echo -e "${BLUE}   Node: $NODE${NC}"
            echo -e "${BLUE}   Port: $PORT${NC}"
            break
        fi
    fi
    
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo ""
        echo -e "${RED}❌ Timeout waiting for VM to start${NC}"
        exit 1
    fi
    
    echo -n "."
    sleep 10
done

# Step 3: Sync code to VM
echo -e "${YELLOW}📤 Syncing code to VM...${NC}"
rsync -avz --progress \
    -e "ssh -p $PORT -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=30" \
    --exclude 'workspace_*' \
    --exclude '.tmp' \
    --exclude '.apptainer_cache' \
    --exclude '*.sif' \
    --exclude '.git' \
    --exclude 'backup_legacy' \
    --exclude 'ref/' \
    --exclude '.snakemake/' \
    --exclude '*.log' \
    --exclude 'dichromat_LOG_*.txt' \
    --exclude '__pycache__' \
    --exclude 'skills/' \
    . "ubuntu@$NODE:~/$BUILD_DIR/"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to sync code to VM${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Code synced${NC}"

# Step 4: Build Docker image on VM
echo -e "${YELLOW}🐳 Building Docker image on VM...${NC}"
ssh -p "$PORT" -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 "ubuntu@$NODE" \
    "cd $BUILD_DIR && sudo docker build -t dichromat:latest . 2>&1"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Docker build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker build complete${NC}"

# Step 5: Convert to SIF
echo -e "${YELLOW}📦 Converting to SIF...${NC}"
ssh -p "$PORT" -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=30 "ubuntu@$NODE" \
    "cd $BUILD_DIR && sudo apptainer build --force $SIF_NAME docker-daemon://dichromat:latest 2>&1"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ SIF conversion failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ SIF created on VM${NC}"

# Step 6: Sync SIF back
echo -e "${YELLOW}📥 Downloading SIF file...${NC}"
rsync -avz --progress \
    -e "ssh -p $PORT -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=30" \
    "ubuntu@$NODE:~/$BUILD_DIR/$SIF_NAME" "./$SIF_NAME"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to download SIF${NC}"
    exit 1
fi
echo -e "${GREEN}✅ SIF downloaded${NC}"

# Success - disable cleanup trap
trap - EXIT

# Final cleanup
echo -e "${YELLOW}🧹 Cleaning up VM...${NC}"
scancel "$JOB_ID" 2>/dev/null || true
rm -f "slurm-${JOB_ID}.out"

echo ""
echo -e "${GREEN}🎉 Build complete!${NC}"
echo -e "${GREEN}   File: $SIF_NAME${NC}"
echo -e "${GREEN}   Size: $(du -h "$SIF_NAME" | cut -f1)${NC}"
echo -e "${GREEN}   Location: $(pwd)/$SIF_NAME${NC}"
