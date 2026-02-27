#!/bin/bash
#
# Build dichromat.sif locally using apptainer fakeroot
# Requires: apptainer with fakeroot support
#

set -e

SIF_NAME="${SIF_NAME:-dichromat.sif}"
PROJECT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"

cd "$PROJECT_DIR"

echo "🐳 Building Docker image..."
docker build -t dichromat:latest .

echo "📦 Converting to SIF with apptainer..."
apptainer build --force --fakeroot "$SIF_NAME" docker-daemon://dichromat:latest

echo "✅ Build complete: $SIF_NAME"
ls -lh "$SIF_NAME"
