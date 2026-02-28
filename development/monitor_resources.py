#!/usr/bin/env python3
"""
Interactive SLURM Resource Monitor with multi-line display.
Shows running jobs and recently completed jobs in real-time.
"""

import subprocess
import sys
import time
import json
import signal
import re
from datetime import datetime, timedelta
from collections import OrderedDict

# ANSI codes
RESET = '\033[0m'
BOLD = '\033[1m'
DIM = '\033[2m'
CLEAR = '\033[2J\033[H'  # Clear screen + move to top
CLEAR_LINE = '\033[K'
CURSOR_UP = '\033[A'
GREEN = '\033[38;5;82m'
YELLOW = '\033[38;5;220m'
RED = '\033[38;5;196m'
BLUE = '\033[38;5;45m'
CYAN = '\033[38;5;51m'

def run_cmd(cmd, timeout=10):
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""

def parse_memory(mem_str):
    if not mem_str or mem_str in ('N/A', ''):
        return '--'
    mem_str = mem_str.strip()
    try:
        if mem_str.endswith('G'):
            return f"{float(mem_str[:-1]):.1f}G"
        elif mem_str.endswith('M'):
            return f"{float(mem_str[:-1])/1024:.1f}G"
        elif mem_str.endswith('K'):
            return f"{float(mem_str[:-1])/1024/1024:.1f}G"
        else:
            val = float(mem_str)
            if val > 1e9:
                return f"{val/1e9:.1f}G"
            elif val > 1e6:
                return f"{val/1e6:.1f}M"
            return f"{val:.0f}K"
    except:
        return mem_str[:6]

def get_running_jobs(user):
    """Get currently running jobs."""
    output = run_cmd(f'squeue -u {user} -t RUNNING --json', timeout=10)
    jobs = []
    try:
        data = json.loads(output)
        for job in data.get('jobs', []):
            job_id = job.get('job_id', 0)
            if isinstance(job_id, dict):
                job_id = job_id.get('number', 0)
            
            run_time = job.get('run_time', 0)
            if isinstance(run_time, dict):
                run_time = run_time.get('number', 0)
            
            hours = run_time // 3600
            minutes = (run_time % 3600) // 60
            secs = run_time % 60
            
            comment = job.get('comment', '')
            if comment and 'rule_' in comment:
                rule_name = comment.replace('rule_', '')
            else:
                rule_name = job.get('name', 'unknown')
            
            # Get real-time stats
            stats = get_job_stats(str(job_id))
            
            jobs.append({
                'jobid': str(job_id),
                'name': rule_name[:35],
                'time': f"{hours}:{minutes:02d}:{secs:02d}",
                'nodes': job.get('nodes', 'unknown')[:15],
                'mem': stats.get('max_rss', '--') if stats else '--',
                'cpu': stats.get('ave_cpu', '--') if stats else '--',
            })
    except Exception:
        pass
    return jobs

def get_job_stats(jobid):
    output = run_cmd(f'sstat -j {jobid}.0 --format=MaxRSS,AveCPU -n -P 2>/dev/null', timeout=3)
    if not output:
        output = run_cmd(f'sstat -j {jobid} --format=MaxRSS,AveCPU -n -P 2>/dev/null', timeout=3)
    if output and '|' in output:
        parts = output.split('|')
        if len(parts) >= 2:
            return {'max_rss': parse_memory(parts[0]), 'ave_cpu': parts[1].strip()}
    return None

def get_completed_jobs(user, window_minutes=5):
    """Get recently completed jobs from sacct."""
    output = run_cmd(
        f'sacct -u {user} --state=COMPLETED,FAILED,CANCELLED,TIMEOUT '
        f'--starttime=now-{window_minutes}minutes '
        f'--format=JobID,JobName%40,State,Elapsed,MaxRSS,ExitCode -n -P '
        f'2>/dev/null | grep -v ".batch" | head -20',
        timeout=5
    )
    
    jobs = []
    seen = set()
    for line in output.split('\n'):
        if not line.strip():
            continue
        parts = line.split('|')
        if len(parts) >= 6:
            jobid = parts[0].split('.')[0]
            if jobid in seen:
                continue
            seen.add(jobid)
            
            name = parts[1]
            if 'rule_' in name:
                name = name.replace('rule_', '')
            
            state = parts[2]
            elapsed = parts[3]
            mem = parse_memory(parts[4])
            exitcode = parts[5]
            
            status = '✓' if state == 'COMPLETED' and exitcode == '0:0' else '✗'
            color = GREEN if status == '✓' else RED
            
            jobs.append({
                'jobid': jobid,
                'name': name[:35],
                'state': state,
                'elapsed': elapsed,
                'mem': mem,
                'status': status,
                'color': color,
            })
    return jobs[:15]  # Keep last 15

class JobMonitor:
    def __init__(self, user, interval=10):
        self.user = user
        self.interval = max(5, interval)
        self.running = []
        self.completed = []
        self.start_time = datetime.now()
        self.last_lines = 0
        
    def clear_display(self):
        """Clear previous output."""
        if self.last_lines > 0:
            sys.stderr.write(CURSOR_UP * self.last_lines + '\r')
            for _ in range(self.last_lines):
                sys.stderr.write(CLEAR_LINE + '\n')
            sys.stderr.write(CURSOR_UP * self.last_lines + '\r')
            sys.stderr.flush()
    
    def format_header(self):
        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split('.')[0]
        
        lines = [
            f"{BLUE}{'='*100}{RESET}",
            f"{BOLD}  SLURM Job Monitor{RESET}  {DIM}User: {self.user} | Elapsed: {elapsed_str}{RESET}",
            f"{BLUE}{'─'*100}{RESET}",
        ]
        return lines
    
    def format_running(self):
        if not self.running:
            return [f"  {DIM}No jobs currently running{RESET}"]
        
        lines = [f"  {BOLD}{CYAN}► RUNNING JOBS ({len(self.running)}){RESET}"]
        lines.append(f"  {DIM}{'Name':<36} {'Time':<10} {'Memory':<10} {'CPU':<12} {'Node'}{RESET}")
        lines.append(f"  {DIM}{'─'*96}{RESET}")
        
        for job in self.running:
            mem_color = GREEN
            if isinstance(job['mem'], str) and 'G' in job['mem']:
                try:
                    val = float(job['mem'].replace('G', ''))
                    if val > 10:
                        mem_color = RED
                    elif val > 1:
                        mem_color = YELLOW
                except:
                    pass
            
            mem_str = f"{mem_color}{job['mem']:<10}{RESET}" if job['mem'] != '--' else f"{DIM}{'--':<10}{RESET}"
            
            lines.append(f"  {job['name']:<36} {job['time']:<10} {mem_str} {job['cpu']:<12} {job['nodes']}")
        
        return lines
    
    def format_completed(self):
        if not self.completed:
            return []
        
        lines = ["", f"  {BOLD}{GREEN}✓ RECENTLY COMPLETED{RESET}"]
        lines.append(f"  {DIM}{'Name':<36} {'Time':<10} {'Memory':<10} {'Status'}{RESET}")
        lines.append(f"  {DIM}{'─'*70}{RESET}")
        
        for job in self.completed[:10]:  # Show last 10
            mem_str = f"{GREEN}{job['mem']:<10}{RESET}" if job['mem'] != '--' else f"{DIM}{'--':<10}{RESET}"
            status_str = f"{job['color']}{job['status']}{RESET}"
            lines.append(f"  {job['name']:<36} {job['elapsed']:<10} {mem_str} {status_str} {job['state']}")
        
        return lines
    
    def format_footer(self):
        return [
            f"{BLUE}{'─'*100}{RESET}",
            f"  {DIM}Update: {datetime.now().strftime('%H:%M:%S')} | Next: {self.interval}s | Ctrl+C to exit{RESET}",
            f"{BLUE}{'='*100}{RESET}",
        ]
    
    def update(self):
        """Update display."""
        # Get fresh data
        self.running = get_running_jobs(self.user)
        self.completed = get_completed_jobs(self.user, window_minutes=10)
        
        # Build display
        lines = []
        lines.extend(self.format_header())
        lines.extend(self.format_running())
        lines.extend(self.format_completed())
        lines.extend(self.format_footer())
        
        # Clear and redraw
        self.clear_display()
        
        output = '\n'.join(lines) + '\n'
        sys.stderr.write(output)
        sys.stderr.flush()
        
        self.last_lines = len(lines)
    
    def run(self):
        """Main loop."""
        def signal_handler(sig, frame):
            print(f"\n{BLUE}Monitor stopped{RESET}")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        print(CLEAR, end='')
        
        try:
            while True:
                self.update()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print(f"\n{BLUE}Monitor stopped{RESET}")

def main():
    if len(sys.argv) < 2:
        user = run_cmd('whoami').strip() or 'unknown'
        interval = 10
    else:
        user = sys.argv[1]
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    monitor = JobMonitor(user, interval)
    monitor.run()

if __name__ == "__main__":
    main()
