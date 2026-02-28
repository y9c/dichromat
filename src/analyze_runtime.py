#!/usr/bin/env python
"""Analyze Snakemake pipeline runtime from main log files."""

import re
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict

def parse_snakemake_log(log_file):
    """Parse the main snakemake log file for timing info."""
    with open(log_file) as f:
        content = f.read()
    
    # Pattern to match rule execution lines
    # [Sat Feb 28 01:53:15 2026]
    # rule filter_eTAM_sites:
    # ...
    # [Sat Feb 28 01:54:44 2026]
    # Error in rule ...
    
    rule_execs = []
    
    # Find all rule blocks
    lines = content.split('\n')
    current_rule = None
    current_start = None
    job_id = None
    
    for i, line in enumerate(lines):
        # Check for timestamp
        ts_match = re.match(r'\[(\w{3} \w{3} \d{1,2} \d{2}:\d{2}:\d{2} \d{4})\]', line)
        if ts_match:
            ts_str = ts_match.group(1)
            ts = datetime.strptime(ts_str, '%a %b %d %H:%M:%S %Y')
            
            # Check if next lines indicate a rule
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                rule_match = re.match(r'rule (\w+):', next_line)
                if rule_match:
                    current_rule = rule_match.group(1)
                    current_start = ts
                    job_id = None
                
                # Check for job submission
                job_match = re.search(r'Submitted batch job (\d+)', next_line)
                if job_match:
                    job_id = job_match.group(1)
            
            # Check if this is an end marker
            if current_rule and current_start:
                if 'Error in rule' in next_line or 'Finished job' in next_line:
                    duration = (ts - current_start).total_seconds()
                    rule_execs.append({
                        'rule': current_rule,
                        'start': current_start,
                        'end': ts,
                        'duration_sec': duration,
                        'duration_min': duration / 60,
                        'duration_hr': duration / 3600,
                        'job_id': job_id,
                        'status': 'error' if 'Error' in next_line else 'success'
                    })
                    current_rule = None
                    current_start = None
    
    return rule_execs

def parse_snakemake_json_log(log_file):
    """Parse snakemake's structured log if available."""
    import json
    
    results = []
    try:
        with open(log_file) as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if data.get('level') == 'run_info':
                        # Snakemake structured log
                        pass
                except:
                    pass
    except:
        pass
    
    return results

def print_summary(rule_execs):
    """Print formatted summary of rule execution times."""
    print("=" * 120)
    print("PIPELINE RUNTIME ANALYSIS")
    print("=" * 120)
    
    if not rule_execs:
        print("No rule executions found in log.")
        return
    
    # Group by rule
    rule_groups = defaultdict(list)
    for r in rule_execs:
        rule_groups[r['rule']].append(r)
    
    # Summary table
    print("\n📊 RULE EXECUTION SUMMARY")
    print("-" * 120)
    print(f"{'Rule':<40} {'Count':<8} {'Total':<12} {'Avg':<12} {'Max':<12} {'Status'}")
    print(f"{'':40} {'':8} {'(hours)':<12} {'(min)':<12} {'(min)':<12}")
    print("-" * 120)
    
    total_time = 0
    sorted_rules = sorted(rule_groups.items(), 
                          key=lambda x: sum(r['duration_sec'] for r in x[1]), 
                          reverse=True)
    
    for rule, runs in sorted_rules:
        count = len(runs)
        total_sec = sum(r['duration_sec'] for r in runs)
        avg_sec = total_sec / count if count > 0 else 0
        max_sec = max(r['duration_sec'] for r in runs) if runs else 0
        total_time += total_sec
        
        errors = sum(1 for r in runs if r['status'] == 'error')
        status = f"✗ {errors} errors" if errors else "✓ OK"
        
        print(f"{rule:<40} {count:<8} {total_sec/3600:<12.2f} {avg_sec/60:<12.1f} {max_sec/60:<12.1f} {status}")
    
    print("-" * 120)
    print(f"{'TOTAL':<40} {'':8} {total_time/3600:<12.2f} {'':12} {'':12}")
    
    # Detailed breakdown
    print("\n📋 DETAILED EXECUTION TIMELINE")
    print("-" * 120)
    print(f"{'Start Time':<25} {'Rule':<35} {'Duration':<15} {'Job ID':<12} {'Status'}")
    print("-" * 120)
    
    for r in sorted(rule_execs, key=lambda x: x['start']):
        status_icon = "✓" if r['status'] == 'success' else "✗"
        job_id = r['job_id'] or "local"
        print(f"{r['start'].strftime('%Y-%m-%d %H:%M:%S'):<25} {r['rule']:<35} "
              f"{r['duration_min']:<15.1f} {job_id:<12} {status_icon}")
    
    # Bottlenecks
    print("\n🐌 BOTTLENECK ANALYSIS")
    print("-" * 120)
    
    long_runs = [r for r in rule_execs if r['duration_min'] > 10]
    long_runs.sort(key=lambda x: x['duration_min'], reverse=True)
    
    if long_runs:
        print(f"{'Rule':<35} {'Duration':<15} {'% of Total':<15} {'Job ID'}")
        print("-" * 120)
        for r in long_runs[:15]:
            pct = (r['duration_sec'] / total_time) * 100 if total_time else 0
            job_id = r['job_id'] or "local"
            print(f"{r['rule']:<35} {r['duration_min']/60:<15.2f} hrs {pct:<15.1f} {job_id}")
        
        print("\n💡 Recommendations:")
        top_bottleneck = long_runs[0]
        print(f"   - Top bottleneck: '{top_bottleneck['rule']}' took {top_bottleneck['duration_min']/60:.1f} hours")
        print(f"     This is {(top_bottleneck['duration_sec']/total_time)*100:.1f}% of total runtime")
        
        if top_bottleneck['rule'] == 'filter_eTAM_sites':
            print(f"\n     filter_eTAM_sites optimization suggestions:")
            print(f"     • Data shows curve_fit is the bottleneck (32 motifs, many failing to converge)")
            print(f"     • Consider: Lower maxfev further, better initial guesses, or skip problematic motifs")
            print(f"     • Current settings: maxfev=10k/20k (was 20k/40k)")
    else:
        print("No runs exceeded 10 minutes")
    
    print("\n" + "=" * 120)

def analyze_resource_usage(workspace):
    """Analyze resource usage from SLURM logs."""
    print("\n📈 RESOURCE USAGE ANALYSIS")
    print("-" * 120)
    
    slurm_dir = Path(workspace) / ".snakemake" / "slurm_logs"
    
    if not slurm_dir.exists():
        print("No SLURM logs found")
        return
    
    for rule_dir in sorted(slurm_dir.glob("rule_*")):
        rule_name = rule_dir.name.replace('rule_', '')
        log_files = list(rule_dir.glob("*.log"))
        
        if log_files:
            # Get latest log
            latest_log = max(log_files, key=lambda p: p.stat().st_mtime)
            
            # Try to extract resource info
            with open(latest_log) as f:
                content = f.read()
            
            # Look for resources line
            mem_match = re.search(r'mem_mb=(\d+)', content)
            threads_match = re.search(r'threads[:=]\s*(\d+)', content)
            
            mem = f"{int(mem_match.group(1))/1024:.0f} GB" if mem_match else "N/A"
            threads = threads_match.group(1) if threads_match else "N/A"
            
            print(f"{rule_name:<40} Threads: {threads:<5} Memory: {mem}")

if __name__ == "__main__":
    workspace = sys.argv[1] if len(sys.argv) > 1 else "workspace_dichromat_run"
    
    # Find the latest main log file
    log_dir = Path(workspace) / ".snakemake" / "log"
    
    if not log_dir.exists():
        print(f"Log directory not found: {log_dir}")
        # Try main project log files
        main_logs = sorted(Path('.').glob('dichromat_LOG_*.txt'), key=lambda p: p.stat().st_mtime, reverse=True)
        if main_logs:
            print(f"Using main log: {main_logs[0]}")
            rule_execs = parse_snakemake_log(main_logs[0])
            print_summary(rule_execs)
    else:
        log_files = sorted(log_dir.glob('*.snakemake.log'), key=lambda p: p.stat().st_mtime, reverse=True)
        
        if log_files:
            latest_log = log_files[0]
            print(f"Analyzing log: {latest_log}")
            rule_execs = parse_snakemake_log(latest_log)
            print_summary(rule_execs)
            analyze_resource_usage(workspace)
        else:
            print(f"No log files found in {log_dir}")
