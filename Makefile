# dichromat Pipeline Build and Publish

# Project Naming
NAME=dichromat
VERSION=$(shell cat VERSION)
DOCKER_ORG=y9c
DOCKER_IMAGE=ghcr.io/$(DOCKER_ORG)/$(NAME)

# Files
SIF_FILE=development/$(NAME).sif
DEF_FILE=$(NAME).def
DOCKER_FILE=Dockerfile

# Cluster Environment
MODULE_INIT=source /data/mgt/modules-5.6.1/init/bash && module load apptainer/1.4.5

.PHONY: all help docker-build docker-push sif build clean test

all: sif ## Build the final .sif image

# --- Docker Targets ---

docker-build: ## Build the Docker image locally
	docker build -t $(NAME):latest -t $(DOCKER_IMAGE):latest -t $(DOCKER_IMAGE):$(VERSION) .

docker-push: ## Push the Docker image to GitHub Container Registry (GHCR)
	docker push $(DOCKER_IMAGE):latest
	docker push $(DOCKER_IMAGE):$(VERSION)

# --- Apptainer Targets ---

# Generate Apptainer definition file from Dockerfile using uvx spython
def: $(DOCKER_FILE) ## Convert Dockerfile to Apptainer definition file (.def)
	uvx spython recipe $(DOCKER_FILE) > $(DEF_FILE)

# Build Apptainer image (.sif) from the local Docker daemon
sif: ## Build Apptainer image (.sif) from the local Docker image
	$(MODULE_INIT) && apptainer build --force --fakeroot $(SIF_FILE) docker-daemon://$(NAME):latest

# Submit a SLURM job to build the SIF (useful for clusters without root/fakeroot)
build: def ## Submit a SLURM job to build the SIF on the cluster
	sbatch --cpus-per-task=64 --mem=64G --partition=normal --job-name=build_$(NAME) --output=build_sif_%j.log --wrap="/bin/bash -c 'export PATH=\"$$PATH\" && export APPTAINER_CACHEDIR=\"$$PWD/.apptainer_cache\" && $(MODULE_INIT) && unset http_proxy https_proxy all_proxy NO_PROXY && apptainer build --force --fakeroot --ignore-fakeroot-command $(SIF_FILE) $(DEF_FILE)'"

# --- Testing & Cleanup ---

test: $(SIF_FILE) ## Run a Snakemake dry-run inside the container
	./run_pipeline.sh -n

clean: ## Remove intermediate build files and temporary data
	rm -f $(DEF_FILE)
	rm -rf .tmp/

help: ## Show this help message
	@echo "$(NAME) Pipeline Build System (v$(VERSION))"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# --- Skill-based Build Targets ---

# Build SIF using skill (auto-detects best method: local fakeroot or VM)
sif-skill: ## Build SIF using skill (auto-detect method)
	python3 development/skills/dichromat-sif-builder/scripts/build_sif.py --method auto --output $(SIF_FILE)

# Build SIF locally (requires fakeroot permission)
sif-local: ## Build SIF locally with fakeroot
	python3 development/skills/dichromat-sif-builder/scripts/build_sif.py --method local --output $(SIF_FILE)

# Build SIF using VM (no root/fakeroot required)
sif-vm: ## Build SIF using temporary VM
	python3 development/skills/dichromat-sif-builder/scripts/build_sif.py --method vm --output $(SIF_FILE)
