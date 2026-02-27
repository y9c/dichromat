# Developer Guide for dichromat

This document provides instructions for maintaining, building, and publishing the `dichromat` pipeline components.

## 🛠 Building the Docker Image

The `dichromat` pipeline uses a multi-stage Docker build to package all dependencies (`samtools`, `hisat-3n`, `countmut`, etc.).

### Build Locally
```bash
make docker-build
```
This creates `dichromat:latest` and tags it with the version found in the `VERSION` file.

## 🚀 Publishing to GitHub Container Registry (GHCR)

The image is hosted at `ghcr.io/y9c/dichromat`.

### 1. Login to GHCR
You will need a GitHub Personal Access Token (PAT) with `write:packages` scope.
```bash
echo $CR_PAT | docker login ghcr.io -u y9c --password-stdin
```

### 2. Push Image
```bash
make docker-push
```

## 📦 Apptainer Conversion

To generate an Apptainer `.sif` image for cluster use from the local Docker daemon:
```bash
make sif
```

To submit a build job to a SLURM cluster (converts Dockerfile to `.def` and builds `.sif`):
```bash
make build
```

## 📜 Versioning
Update the `VERSION` file in the root directory before publishing a new release.
```bash
echo "0.1.1" > VERSION
```
