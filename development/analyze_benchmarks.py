#!/usr/bin/env python
"""Analyze Snakemake benchmark files for runtime and resource usage."""

import sys
import re
from pathlib import Path
from collections import defaultdict

def parse_benchmark_file(filepath):
    """Parse a snakemake benchmark file."""
    try:
        lines = filepath.read_text().strip().split('\n')
        if len(lines) < 2:
            return None
        
        header = lines[0].split('\t')
        values = lines[1].split('\t')
        
        data = dict(zip(header, values))
        
        # Convert types
        return {
            's': float(data.get('s', 0)),
            'h:m:s': data.get('h:m:s', '0:00:00'),
            'max_rss_mb': float(data.get('max_rss', 0)),
            'max_vms_mb': float(data.get('max_vms', 0)),
            'max_uss_mb': float(data.get('max_uss', 0)),
            'max_pss_mb': float(data.get('max_pss', 0)),
            'io_in_mb': float(data.get('io_in', 0)),
            'io_out_mb': float(data.get('io_out', 0)),
            'mean_load': float(data.get('mean_load', 0)),
            'cpu_time': float(data.get('cpu_time', 0)),
        }
    except Exception:
        return None


def extract_rule_name(filename):
    """Extract rule name from benchmark filename.
    
    Format: {rulename}_{wildcard1}_{wildcard2}...benchmark.txt
    We need to extract just the rulename part.
    """
    # Remove .benchmark.txt
    name = filename.replace('.benchmark.txt', '')
    
    # Common rule patterns (try to match full rule names)
    # Sort by length (longest first) to match most specific first
    common_patterns = [
        'mainmap_align_se', 'mainmap_align_pe',
        'premap_align_se', 'premap_align_pe', 
        'premap_get_unmapped_se', 'premap_get_unmapped_pe',
        'mainmap_get_unmapped_se', 'mainmap_get_unmapped_pe',
        'remap_align_se', 'remap_align_pe',
        'premap_fixmate_se', 'premap_fixmate_pe',
        'drop_duplicates', 'mark_duplicates',
        'combine_contamination_fa', 'combine_genes_fa',
        'build_contamination_hisat3n_index', 'build_hisat3n_index',
        'prepared_transcript_ref', 'prepared_gene_ref',
        'finalize_trim_report', 'finalize_premap_bam',
        'finalize_mainmap_transcript_bam', 'finalize_mainmap_genes_bam',
        'finalize_remap_summary', 'finalize_premap_summary',
        'finalize_mainmap_summary', 'finalize_discarded_reads',
        'aggregate_multiqc_stats', 'generate_site_report',
        'generate_mapping_report', 'count_reads',
        'premap_get_unmapped', 'mainmap_get_unmapped',
        'trim_se', 'trim_pe',
        'combine_bams', 'stat_combined',
        'stat_dedup', 'stat_count',
        'qc_trimmed', 'report_qc_trimmed',
        'pileup_base', 'join_pileup_table',
        'merge_gene_and_genome_table', 'filter_eTAM_sites',
        'prepared', 'mainmap', 'premap', 'combine',
        'finalize', 'build', 'report', 'trim', 'stat', 'qc', 'drop',
    ]
    
    for pattern in common_patterns:
        if name.startswith(pattern + '_') or name == pattern:
            return pattern
    
    # Fallback: take everything before first {wildcard}
    # Wildcards usually start with sample/run numbers
    parts = name.split('_')
    
    # Try to find where wildcards start (looking for patterns like sample*, run*, etc.)
    for i, part in enumerate(parts):
        if re.match(r'^(sample|run|batch|lib|lane|chunk|part)\d+', part, re.I):
            return '_'.join(parts[:i]) if i > 0 else name
    
    # If no wildcards found, return full name
    return name


def analyze_benchmarks(benchmark_dir):
    """Analyze all benchmark files in directory."""
    benchmark_dir = Path(benchmark_dir)
    
    if not benchmark_dir.exists():
        print(f"Benchmark directory not found: {benchmark_dir}")
        return
    
    # Find all benchmark files
    benchmark_files = list(benchmark_dir.glob('*.benchmark.txt'))
    
    if not benchmark_files:
        print(f"No benchmark files found in {benchmark_dir}")
        return
    
    print(f"Found {len(benchmark_files)} benchmark files")
    
    # Parse and group by rule
    rule_stats = defaultdict(list)
    
    for bf in benchmark_files:
        # Extract rule name from filename
        rule_name = extract_rule_name(bf.stem)
        
        data = parse_benchmark_file(bf)
        if data:
            rule_stats[rule_name].append({
                'file': str(bf.name),
                'data': data
            })
    
    # Print summary
    print("\n" + "="*120)
    print("PIPELINE RESOURCE USAGE ANALYSIS")
    print("="*120)
    
    # Sort by total time
    sorted_rules = sorted(rule_stats.items(), 
                          key=lambda x: sum(r['data']['s'] for r in x[1]),
                          reverse=True)
    
    print(f"\n{'Rule':<50} {'Count':<8} {'Total (min)':<15} {'Avg (min)':<15} {'Max (min)':<15} {'Avg RSS':<10}")
    print("-"*130)
    
    total_time = 0
    total_mem = 0
    
    for rule, runs in sorted_rules:
        count = len(runs)
        times = [r['data']['s'] for r in runs]
        mems = [r['data']['max_rss_mb'] for r in runs]
        
        total_s = sum(times)
        avg_s = total_s / count if count > 0 else 0
        max_s = max(times) if times else 0
        avg_mem = sum(mems) / len(mems) if mems else 0
        
        total_time += total_s
        total_mem = max(total_mem, max(mems) if mems else 0)
        
        print(f"{rule:<50} {count:<8} {total_s/60:<15.1f} {avg_s/60:<15.1f} {max_s/60:<15.1f} {avg_mem/1024:<10.1f}GB")
    
    print("-"*130)
    print(f"{'TOTAL':<50} {'':8} {total_time/60:<15.1f} {'':15} {'':15} {total_mem/1024:<10.1f}GB")
    
    # Detailed breakdown
    print("\n" + "="*120)
    print("DETAILED BREAKDOWN")
    print("="*120)
    
    print(f"\n{'Rule':<50} {'Time (min)':<12} {'RSS (GB)':<12} {'VMS (GB)':<12} {'CPU%':<10} {'I/O (MB)':<15}")
    print("-"*130)
    
    for rule, runs in sorted_rules:
        for run in runs:
            d = run['data']
            io_total = d['io_in_mb'] + d['io_out_mb']
            # Truncate rule name for display
            rule_display = rule[:49] if len(rule) > 49 else rule
            print(f"{rule_display:<50} {d['s']/60:<12.1f} {d['max_rss_mb']/1024:<12.1f} "
                  f"{d['max_vms_mb']/1024:<12.1f} {d['mean_load']:<10.1f} {io_total:<15.1f}")
    
    # Bottlenecks
    print("\n" + "="*120)
    print("BOTTLENECKS (jobs > 10 minutes)")
    print("="*120)
    
    slow_jobs = []
    for rule, runs in rule_stats.items():
        for run in runs:
            if run['data']['s'] > 600:  # 10 minutes
                slow_jobs.append({
                    'rule': rule,
                    'time_min': run['data']['s'] / 60,
                    'mem_gb': run['data']['max_rss_mb'] / 1024,
                    'cpu': run['data']['mean_load'],
                    'file': run['file']
                })
    
    slow_jobs.sort(key=lambda x: x['time_min'], reverse=True)
    
    if slow_jobs:
        print(f"{'Rule':<50} {'Time (min)':<15} {'Memory (GB)':<15} {'CPU%':<10}")
        print("-"*100)
        for job in slow_jobs[:20]:
            rule_display = job['rule'][:49] if len(job['rule']) > 49 else job['rule']
            print(f"{rule_display:<50} {job['time_min']:<15.1f} {job['mem_gb']:<15.1f} {job['cpu']:<10.1f}")
    else:
        print("No jobs exceeded 10 minutes")
    
    print("="*120)


if __name__ == "__main__":
    benchmark_dir = sys.argv[1] if len(sys.argv) > 1 else ".snakemake/benchmarks"
    analyze_benchmarks(benchmark_dir)
