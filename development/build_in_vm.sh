#!/bin/bash
#
# Build dichromat.sif using a temporary VM with Docker support
# Usage: ./build_in_vm.sh [sif_name]
#

SIF_NAME=${1:-dichromat.sif}
VM_SCRIPT="/data/share/vm/submit_vm.sh"
SSH_KEY="${HOME}/.ssh/id_vm_rsa"
BUILD_DIR="pipeline_build"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting VM-based build process...${NC}"

# Step 1: Submit VM job
echo -e "${YELLOW}📤 Submitting VM job to SLURM...${NC}"
JOB_OUTPUT=$(sbatch --account=lab-changye --cpus-per-task=128 --mem=400G "${VM_SCRIPT}" 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to submit VM job:${NC}"
    echo "$JOB_OUTPUT"
    exit 1
fi

JOB_ID=$(echo "$JOB_OUTPUT" | grep -oP 'Submitted batch job \K\d+')
echo -e "${GREEN}✅ VM job submitted: $JOB_ID${NC}"

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
            echo -e "${GREEN}✅ VM ready! Node: $NODE, Port: $PORT${NC}"
            break
        fi
    fi
    
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo -e "${RED}❌ Timeout waiting for VM to start${NC}"
        exit 1
    fi
    
    sleep 10
    echo -n "."
done

echo ""

# Step 3: Sync code to VM
echo -e "${YELLOW}📤 Syncing code to VM...${NC}"
rsync -avz -e "ssh -p $PORT -i $SSH_KEY -o StrictHostKeyChecking=no" \
    --exclude 'workspace_*' --exclude '.tmp' --exclude '.apptainer_cache' \
    --exclude '*.sif' --exclude '.git' --exclude 'backup_legacy' \
    --exclude 'ref/' --exclude '.snakemake/' --exclude '*.log' \
    --exclude 'dichromat_LOG_*.txt' --exclude '__pycache__' \
    . "ubuntu@$NODE:~/$BUILD_DIR/"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to sync code to VM${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Code synced${NC}"

# Step 4: Build Docker image on VM
echo -e "${YELLOW}🐳 Building Docker image on VM...${NC}"
ssh -p "$PORT" -i "$SSH_KEY" -o StrictHostKeyChecking=no "ubuntu@$NODE" \
    "cd $BUILD_DIR && sudo docker build -t dichromat:latest ."

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Docker build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Docker build complete${NC}"

# Step 5: Convert to SIF
echo -e "${YELLOW}📦 Converting to SIF...${NC}"
ssh -p "$PORT" -i "$SSH_KEY" -o StrictHostKeyChecking=no "ubuntu@$NODE" \
    "cd $BUILD_DIR && sudo apptainer build --force $SIF_NAME docker-daemon://dichromat:latest"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ SIF conversion failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ SIF created${NC}"

# Step 6: Sync SIF back
echo -e "${YELLOW}📥 Downloading SIF file...${NC}"
rsync -avz -e "ssh -p $PORT -i $SSH_KEY -o StrictHostKeyChecking=no" \
    "ubuntu@$NODE:~/$BUILD_DIR/$SIF_NAME" "./$SIF_NAME"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to download SIF${NC}"
    exit 1
fi
echo -e "${GREEN}✅ SIF downloaded${NC}"

# Step 7: Cleanup
echo -e "${YELLOW}🧹 Cleaning up VM...${NC}"
scancel "$JOB_ID"
echo -e "${GREEN}✅ VM job cancelled${NC}"

echo ""
echo -e "${GREEN}🎉 Build complete: $SIF_NAME${NC}"
echo -e "${GREEN}   Size: $(du -h "$SIF_NAME" | cut -f1)${NC}"
