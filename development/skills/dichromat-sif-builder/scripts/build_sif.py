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
PROJECT_ROOT = SKILL_DIR.parent.parent


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


def run_local_build():
    """Run local fakeroot build"""
    script = SKILL_DIR / "scripts" / "build_local.sh"
    print("🔧 Running local build with fakeroot...")
    return subprocess.run(["bash", str(script)], cwd=PROJECT_ROOT).returncode


def run_vm_build():
    """Run VM-based build"""
    script = SKILL_DIR / "scripts" / "build_vm.sh"
    print("🖥️  Running VM-based build...")
    return subprocess.run(["bash", str(script)], cwd=PROJECT_ROOT).returncode


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
        default="dichromat.sif",
        help="Output SIF filename (default: dichromat.sif)"
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
    
    # Execute build
    if method == "local":
        ret = run_local_build()
    else:
        ret = run_vm_build()
    
    # Check result
    sif_path = PROJECT_ROOT / args.output
    if ret == 0 and sif_path.exists():
        size = sif_path.stat().st_size / (1024 * 1024)  # MB
        print(f"\n✅ Build successful: {args.output} ({size:.1f} MB)")
        return 0
    else:
        print(f"\n❌ Build failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
