#!/usr/bin/env python
"""Real-time resource monitor for Snakemake pipeline.

Monitors running SLURM jobs and reports RSS/CPU usage in real-time.
"""

import sys
import time
import subprocess
import json
from datetime import datetime
from collections import defaultdict

def get_running_jobs(user):
    """Get list of running jobs for user."""
    try:
        result = subprocess.run(
            ['squeue', '-u', user, '-t', 'RUNNING', '-o', '%i %j %C %m'],
            capture_output=True, text=True, timeout=5
        )
        lines = result.stdout.strip().split('\n')[1:]  # Skip header
        jobs = []
        for line in lines:
            parts = line.split()
            if len(parts) >= 4:
                jobs.append({
                    'jobid': parts[0],
                    'name': parts[1],
                    'cpus': parts[2],
                    'mem': parts[3]
                })
        return jobs
    except Exception as e:
        return []

def get_job_stats(jobid):
    """Get real-time stats for a SLURM job using sstat."""
    try:
        # Get step stats (more accurate than job stats)
        result = subprocess.run(
            ['sstat', '-j', f'{jobid}.0', '--format=JobID,MaxRSS,AveCPU,MaxVMSize', '-n', '-P'],
            capture_output=True, text=True, timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split('|')
            if len(parts) >= 4:
                return {
                    'jobid': parts[0],
                    'max_rss': parts[1].strip() if parts[1].strip() else 'N/A',
                    'ave_cpu': parts[2].strip() if parts[2].strip() else 'N/A',
                    'max_vms': parts[3].strip() if parts[3].strip() else 'N/A'
                }
        
        # Fallback to job stats
        result = subprocess.run(
            ['sstat', '-j', jobid, '--format=JobID,MaxRSS,AveCPU,MaxVMSize', '-n', '-P'],
            capture_output=True, text=True, timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split('|')
            if len(parts) >= 4:
                return {
                    'jobid': parts[0],
                    'max_rss': parts[1].strip() if parts[1].strip() else 'N/A',
                    'ave_cpu': parts[2].strip() if parts[2].strip() else 'N/A',
                    'max_vms': parts[3].strip() if parts[3].strip() else 'N/A'
                }
    except Exception as e:
        pass
    
    return None

def format_memory(mem_str):
    """Format memory string to GB."""
    if not mem_str or mem_str == 'N/A':
        return 'N/A'
    
    try:
        # Handle formats like "1.5G", "1500M", "1500000K"
        mem_str = mem_str.strip()
        if mem_str.endswith('G'):
            return f"{float(mem_str[:-1]):.1f}GB"
        elif mem_str.endswith('M'):
            return f"{float(mem_str[:-1])/1024:.1f}GB"
        elif mem_str.endswith('K'):
            return f"{float(mem_str[:-1])/1024/1024:.1f}GB"
        else:
            # Assume bytes
            val = float(mem_str)
            if val > 1e9:
                return f"{val/1e9:.1f}GB"
            elif val > 1e6:
                return f"{val/1e6:.1f}MB"
            else:
                return f"{val/1e3:.1f}KB"
    except:
        return mem_str

def monitor_loop(user, interval=30):
    """Main monitoring loop."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting resource monitor...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking every {interval}s (Press Ctrl+C to stop)\n")
    
    # Track peak values
    peak_stats = defaultdict(lambda: {'max_rss': '0', 'max_vms': '0'})
    
    try:
        while True:
            jobs = get_running_jobs(user)
            
            if not jobs:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No running jobs", end='\r')
                time.sleep(interval)
                continue
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Active Jobs: {len(jobs)}")
            print(f"{'JobID':<12} {'Name':<30} {'CPUs':<8} {'Peak RSS':<12} {'Peak VMS':<12} {'Ave CPU'}")
            print("-" * 100)
            
            for job in jobs:
                stats = get_job_stats(job['jobid'])
                
                if stats:
                    # Update peaks
                    if stats['max_rss'] != 'N/A':
                        peak_stats[job['jobid']]['max_rss'] = stats['max_rss']
                    if stats['max_vms'] != 'N/A':
                        peak_stats[job['jobid']]['max_vms'] = stats['max_vms']
                    
                    print(f"{job['jobid']:<12} {job['name'][:29]:<30} {job['cpus']:<8} "
                          f"{format_memory(peak_stats[job['jobid']]['max_rss']):<12} "
                          f"{format_memory(peak_stats[job['jobid']]['max_vms']):<12} "
                          f"{stats['ave_cpu']}")
                else:
                    print(f"{job['jobid']:<12} {job['name'][:29]:<30} {job['cpus']:<8} "
                          f"{'N/A':<12} {'N/A':<12} {'N/A'}")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n\n[{datetime.now().strftime('%H:%M:%S')}] Monitor stopped")
        
        # Print final summary
        if peak_stats:
            print("\n=== PEAK RESOURCE USAGE SUMMARY ===")
            print(f"{'JobID':<12} {'Peak RSS':<15} {'Peak VMS':<15}")
            print("-" * 45)
            for jobid, stats in sorted(peak_stats.items()):
                print(f"{jobid:<12} {format_memory(stats['max_rss']):<15} {format_memory(stats['max_vms']):<15}")

if __name__ == "__main__":
    import os
    user = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('USER', 'chye')
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    
    monitor_loop(user, interval)
