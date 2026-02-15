#!/usr/bin/env python3
"""
Monitor script for ArXiv dataset merging.
Watches the log file and reports progress and completion.
"""
import time
import re
import subprocess
import os
import sys
from pathlib import Path

LOG_FILE = "/home/kurtluo/yannan/jmp/logs/merge_arxiv_datasets.log"
SCRIPT_FILE = "/home/kurtluo/yannan/jmp/src/01_data_construction/merge_arxiv_datasets.py"
OUTPUT_FILE = "/home/kurtluo/yannan/jmp/data/processed/publication/merged_arxiv_complete.parquet"
CHECK_INTERVAL = 15  # Check every 15 seconds

def get_process_pid():
    """Get the PID of the running merge script"""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "merge_arxiv_datasets.py"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                try:
                    with open(f"/proc/{pid}/cmdline", 'r') as f:
                        cmdline = f.read()
                        if 'python' in cmdline and 'merge_arxiv_datasets.py' in cmdline:
                            return int(pid)
                except:
                    continue
    except:
        pass
    return None

def check_log_for_completion(log_file):
    """Check if merge is complete"""
    if not os.path.exists(log_file):
        return False, None
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Check for completion indicators
        completion_patterns = [
            r"MERGE COMPLETE",
            r"Saved to:",
            r"File size:",
        ]
        
        for pattern in completion_patterns:
            if re.search(pattern, content):
                # Extract key statistics
                stats = {}
                
                # Extract final dataset size
                match = re.search(r"Final dataset size: ([\d,]+) papers", content)
                if match:
                    stats['total_papers'] = match.group(1)
                
                # Extract file size
                match = re.search(r"File size: ([\d.]+) MB", content)
                if match:
                    stats['file_size_mb'] = match.group(1)
                
                # Extract date range
                match = re.search(r"Date range: (.+?) to (.+?)", content)
                if match:
                    stats['date_range'] = f"{match.group(1)} to {match.group(2)}"
                
                # Extract abstract coverage
                match = re.search(r"Abstract coverage: ([\d.]+)%", content)
                if match:
                    stats['abstract_coverage'] = match.group(1)
                
                return True, stats
        
        return False, None
    except Exception as e:
        print(f"Error reading log file: {e}")
        return False, None

def get_latest_log_lines(log_file, n=10):
    """Get the last n lines from log file"""
    if not os.path.exists(log_file):
        return []
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        return lines[-n:] if len(lines) > n else lines
    except:
        return []

def main():
    """Main monitoring loop"""
    print("="*80)
    print("ARXIV MERGE MONITOR")
    print("="*80)
    print(f"Monitoring log: {LOG_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Check interval: {CHECK_INTERVAL} seconds")
    print("Press Ctrl+C to stop monitoring\n")
    
    last_size = 0
    start_time = time.time()
    
    try:
        while True:
            # Check if process is running
            pid = get_process_pid()
            if not pid:
                print(f"[{time.strftime('%H:%M:%S')}] Process not running")
                # Check if it completed
                is_complete, stats = check_log_for_completion(LOG_FILE)
                if is_complete:
                    print("\n" + "="*80)
                    print("MERGE COMPLETED!")
                    print("="*80)
                    if stats:
                        print("\nResults:")
                        for key, value in stats.items():
                            print(f"  {key}: {value}")
                    
                    # Check if output file exists
                    if os.path.exists(OUTPUT_FILE):
                        file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
                        print(f"\nOutput file: {OUTPUT_FILE}")
                        print(f"File size: {file_size:.2f} MB")
                    
                    print("\n" + "="*80)
                    break
                else:
                    print("  Waiting for process to start or checking completion...")
                    time.sleep(CHECK_INTERVAL)
                    continue
            
            # Check log file for new content
            if os.path.exists(LOG_FILE):
                current_size = os.path.getsize(LOG_FILE)
                if current_size > last_size:
                    last_size = current_size
                    elapsed = time.time() - start_time
                    print(f"[{time.strftime('%H:%M:%S')}] Log updated ({current_size} bytes, {elapsed/60:.1f} min elapsed)")
                    
                    # Show latest progress
                    latest_lines = get_latest_log_lines(LOG_FILE, 3)
                    for line in latest_lines:
                        line = line.strip()
                        if line and any(keyword in line for keyword in ['Loading', 'Step', 'rows', 'papers', 'After']):
                            print(f"  → {line}")
            
            # Check if analysis is complete
            is_complete, stats = check_log_for_completion(LOG_FILE)
            if is_complete:
                print("\n" + "="*80)
                print("MERGE COMPLETED!")
                print("="*80)
                if stats:
                    print("\nResults:")
                    for key, value in stats.items():
                        print(f"  {key}: {value}")
                
                # Check if output file exists
                if os.path.exists(OUTPUT_FILE):
                    file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
                    print(f"\nOutput file: {OUTPUT_FILE}")
                    print(f"File size: {file_size:.2f} MB")
                
                print("\n" + "="*80)
                break
            
            time.sleep(CHECK_INTERVAL)
    
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
    except Exception as e:
        print(f"\n\nMonitoring error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
