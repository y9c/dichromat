# dichromat-sif-builder

Build the `dichromat.sif` Apptainer image for the dichromat pipeline.

## Overview

This skill builds the container image using either:
- **Local build**: Direct `apptainer build` (requires fakeroot/root)
- **VM build**: Docker inside a temporary SLURM VM (no root needed)

## Quick Start

```bash
# Build using available method (auto-detect)
python3 skills/dichromat-sif-builder/scripts/build_sif.py

# Or specify method explicitly
python3 skills/dichromat-sif-builder/scripts/build_sif.py --method auto    # Default
python3 skills/dichromat-sif-builder/scripts/build_sif.py --method local   # Local fakeroot
python3 skills/dichromat-sif-builder/scripts/build_sif.py --method vm      # VM-based
```

## Build Methods

### Method 1: Local Build (Fastest)

Requires: `apptainer` with fakeroot support

```bash
# Check if fakeroot is available
apptainer build --fakeroot test.sif docker://alpine:latest 2>/dev/null && echo "fakeroot OK"

# Build locally
bash skills/dichromat-sif-builder/scripts/build_local.sh
```

**Output**: `dichromat.sif` in project root

### Method 2: VM Build (No Root Required)

Requires: SLURM access to VM nodes

```bash
# Launch VM and build
bash skills/dichromat-sif-builder/scripts/build_vm.sh
```

**Process**:
1. Submits SLURM job for VM (128 cores, 400G RAM)
2. Waits for VM ready (SSH port)
3. Syncs code to VM
4. Builds Docker image inside VM
5. Converts to SIF
6. Downloads SIF back
7. Cleans up VM

**Output**: `dichromat.sif` in project root

## Makefile Integration

Add to your Makefile:

```makefile
SIF_FILE := dichromat.sif
BUILD_SCRIPT := skills/dichromat-sif-builder/scripts/build_sif.py

# Auto-detect best build method
sif:
	python3 $(BUILD_SCRIPT) --method auto

# Force local build
sif-local:
	python3 $(BUILD_SCRIPT) --method local

# Force VM build
sif-vm:
	python3 $(BUILD_SCRIPT) --method vm

# Check if SIF is up to date
sif-check:
	@if [ ! -f $(SIF_FILE) ] || [ Dockerfile -nt $(SIF_FILE) ]; then \
		echo "SIF needs rebuild"; \
		python3 $(BUILD_SCRIPT) --method auto; \
	else \
		echo "SIF is up to date"; \
	fi
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `fakeroot not available` | Use VM method: `--method vm` |
| `ssh connection refused` | Wait for VM to fully boot (check `slurm-*.out`) |
| `docker build fails` | Check VM has internet access |
| `rsync permission denied` | Verify SSH key at `~/.ssh/id_vm_rsa` |

## Configuration

Environment variables:
- `SIF_NAME`: Output filename (default: `dichromat.sif`)
- `VM_ACCOUNT`: SLURM account (default: `lab-changye`)
- `VM_CPUS`: VM CPU cores (default: `128`)
- `VM_MEM`: VM memory (default: `400G`)

## Files

```
skills/dichromat-sif-builder/
├── SKILL.md                 # This file
├── scripts/
│   ├── build_sif.py         # Main entry point (auto-detect)
│   ├── build_local.sh       # Local fakeroot build
│   └── build_vm.sh          # VM-based build
└── assets/
    └── (none)
```
