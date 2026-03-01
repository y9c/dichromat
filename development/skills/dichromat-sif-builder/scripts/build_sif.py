#!/usr/bin/env python3
"""
dichromat SIF Builder - Main entry point
Auto-detects best build method and executes
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

SKILL_DIR = Path(__file__).parent.parent
# SKILL_DIR is development/skills/dichromat-sif-builder
# PROJECT_ROOT is 3 levels up: skills -> development -> project root
PROJECT_ROOT = SKILL_DIR.parent.parent.parent


def check_fakeroot():
    """Check if apptainer fakeroot is available"""
    try:
        result = subprocess.run(
            ["apptainer", "build", "--fakeroot", "--help"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0 and "fakeroot" in result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def run_local_build(output):
    """Run local fakeroot build"""
    script = SKILL_DIR / "scripts" / "build_local.sh"
    print("🔧 Running local build with fakeroot...")
    return subprocess.run(["bash", str(script), str(output)], cwd=PROJECT_ROOT).returncode


def run_vm_build(output):
    """Run VM-based build"""
    script = SKILL_DIR / "scripts" / "build_vm.sh"
    print("🖥️  Running VM-based build...")
    return subprocess.run(["bash", str(script), str(output)], cwd=PROJECT_ROOT).returncode


def main():
    parser = argparse.ArgumentParser(description="Build dichromat.sif container")
    parser.add_argument(
        "--method",
        choices=["auto", "local", "vm"],
        default="auto",
        help="Build method (default: auto)"
    )
    parser.add_argument(
        "--output",
        default="development/dichromat.sif",
        help="Output SIF filename relative to project root (default: development/dichromat.sif)"
    )
    
    args = parser.parse_args()
    
    os.chdir(PROJECT_ROOT)
    
    # Determine method
    method = args.method
    if method == "auto":
        if check_fakeroot():
            print("✅ Fakeroot available - using local build")
            method = "local"
        else:
            print("⚠️  Fakeroot not available - using VM build")
            method = "vm"
    
    # Ensure output directory exists
    output_path = Path(args.output)
    if output_path.parent:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
    # Execute build
    if method == "local":
        ret = run_local_build(args.output)
    else:
        ret = run_vm_build(args.output)
    
    # Check result
    sif_path = PROJECT_ROOT / args.output
    if ret == 0 and sif_path.exists():
        size = sif_path.stat().st_size / (1024 * 1024)  # MB
        print(f"\n✅ Build successful: {args.output} ({size:.1f} MB)")
        
        # Suggest Zenodo upload
        zenodo_script = PROJECT_ROOT / "development" / "scripts" / "upload_to_zenodo.py"
        if zenodo_script.exists():
            print("\n📤 To upload to Zenodo, run:")
            print("   export ZENODO_TOKEN=your_token_here")
            print("   cd development/scripts && uv run python upload_to_zenodo.py")
            
        return 0
    else:
        print(f"\n❌ Build failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
