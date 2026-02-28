#!/usr/bin/env python
"""
Snakemake log handler for tracking execution time and resources.
Usage: snakemake --log-handler-script src/log_handler.py ...
"""

import json
import sys
from datetime import datetime
from collections import defaultdict

# Store timing data
job_timings = {}
rule_stats = defaultdict(lambda: {
    'count': 0,
    'total_time': 0,
    'max_time': 0,
    'min_time': float('inf'),
    'resources': []
})

def log_handler(msg):
    """Process log messages from Snakemake."""
    global job_timings, rule_stats
    
    level = msg.get('level', '')
    
    # Track job start
    if level == 'job_info':
        jobid = msg.get('jobid')
        rule = msg.get('name', 'unknown')
        threads = msg.get('threads', 1)
        resources = msg.get('resources', {})
        
        job_timings[jobid] = {
            'rule': rule,
            'start_time': datetime.now(),
            'threads': threads,
            'resources': resources,
            'input': msg.get('input', []),
            'output': msg.get('output', [])
        }
    
    # Track job finish
    elif level == 'job_finished':
        jobid = msg.get('job_id')
        if jobid in job_timings:
            job_data = job_timings[jobid]
            start_time = job_data['start_time']
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            rule = job_data['rule']
            stats = rule_stats[rule]
            stats['count'] += 1
            stats['total_time'] += duration
            stats['max_time'] = max(stats['max_time'], duration)
            stats['min_time'] = min(stats['min_time'], duration)
            stats['resources'].append({
                'threads': job_data['threads'],
                'resources': job_data['resources'],
                'duration_sec': duration,
                'duration_min': duration / 60,
                'duration_hr': duration / 3600
            })
            
            # Print real-time update
            print(f"[TIMING] Rule '{rule}' (job {jobid}) finished in {duration/60:.1f} min", 
                  file=sys.stderr)
            
            del job_timings[jobid]
    
    # Track job error
    elif level == 'job_error':
        jobid = msg.get('jobid')
        if jobid in job_timings:
            job_data = job_timings[jobid]
            start_time = job_data['start_time']
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            rule = job_data['rule']
            print(f"[TIMING] Rule '{rule}' (job {jobid}) FAILED after {duration/60:.1f} min",
                  file=sys.stderr)
            
            del job_timings[jobid]
    
    # Print summary at workflow end
    elif level == 'info' and 'Finished job' in msg.get('msg', ''):
        print_summary()

def print_summary():
    """Print timing summary."""
    if not rule_stats:
        return
    
    print("\n" + "="*100, file=sys.stderr)
    print("PIPELINE EXECUTION SUMMARY", file=sys.stderr)
    print("="*100, file=sys.stderr)
    
    # Sort by total time
    sorted_rules = sorted(rule_stats.items(), 
                          key=lambda x: x[1]['total_time'], 
                          reverse=True)
    
    print(f"\n{'Rule':<40} {'Count':<8} {'Total (min)':<15} {'Avg (min)':<15} {'Max (min)':<15}", 
          file=sys.stderr)
    print("-"*100, file=sys.stderr)
    
    total_time = 0
    for rule, stats in sorted_rules:
        count = stats['count']
        total = stats['total_time'] / 60
        avg = total / count if count > 0 else 0
        max_t = stats['max_time'] / 60
        total_time += stats['total_time']
        
        print(f"{rule:<40} {count:<8} {total:<15.1f} {avg:<15.1f} {max_t:<15.1f}",
              file=sys.stderr)
    
    print("-"*100, file=sys.stderr)
    print(f"{'TOTAL':<40} {'':<8} {total_time/60:<15.1f}", file=sys.stderr)
    
    # Bottlenecks
    print("\n" + "="*100, file=sys.stderr)
    print("BOTTLENECKS (> 5 minutes)", file=sys.stderr)
    print("="*100, file=sys.stderr)
    
    bottlenecks = [(r, s) for r, s in sorted_rules if s['max_time'] > 300]
    if bottlenecks:
        print(f"{'Rule':<40} {'Duration':<15} {'Resources'}", file=sys.stderr)
        print("-"*100, file=sys.stderr)
        for rule, stats in bottlenecks:
            for res in stats['resources']:
                if res['duration_sec'] > 300:
                    mem = res['resources'].get('mem_mb', 'N/A')
                    if isinstance(mem, (int, float)):
                        mem = f"{mem/1024:.0f}GB"
                    print(f"{rule:<40} {res['duration_min']:<15.1f} {res['threads']} cores, {mem}",
                          file=sys.stderr)
    else:
        print("No bottlenecks found", file=sys.stderr)
    
    print("="*100 + "\n", file=sys.stderr)

# This function is called by Snakemake
def log_handler(msg):
    try:
        process_log(msg)
    except Exception as e:
        # Don't let log handler break the pipeline
        pass

def process_log(msg):
    """Process log messages."""
    import sys
    from datetime import datetime
    
    # Get message fields
    level = msg.get('level', '')
    
    # Write timing info to a dedicated file
    with open('timing_log.jsonl', 'a') as f:
        if level in ['job_info', 'job_finished', 'job_error']:
            entry = {
                'timestamp': datetime.now().isoformat(),
                'level': level,
                'jobid': msg.get('jobid') or msg.get('job_id'),
                'rule': msg.get('name', msg.get('rule', 'unknown')),
                'threads': msg.get('threads', 1),
                'resources': msg.get('resources', {}),
            }
            f.write(json.dumps(entry) + '\n')

# Snakemake calls this function for each log message
if __name__ == '__main__':
    # For testing
    test_msg = {'level': 'job_info', 'jobid': 1, 'name': 'test_rule', 'threads': 4}
    log_handler(test_msg)
