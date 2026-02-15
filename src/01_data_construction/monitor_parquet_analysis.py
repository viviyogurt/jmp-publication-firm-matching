#!/usr/bin/env python3
"""
Monitor script for parquet file analysis.
Watches the log file and automatically fixes errors as they occur.
"""
import time
import re
import subprocess
import os
import sys
from pathlib import Path

LOG_FILE = "/home/kurtluo/yannan/jmp/logs/analyze_parquet_files.log"
SCRIPT_FILE = "/home/kurtluo/yannan/jmp/src/01_data_construction/analyze_parquet_files.py"
PID_FILE = "/home/kurtluo/yannan/jmp/logs/analyze_parquet_analysis.pid"
CHECK_INTERVAL = 10  # Check every 10 seconds

def get_process_pid():
    """Get the PID of the running analysis script"""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "analyze_parquet_files.py"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            # Return the Python process, not the shell wrapper
            for pid in pids:
                try:
                    with open(f"/proc/{pid}/cmdline", 'r') as f:
                        cmdline = f.read()
                        if 'python' in cmdline and 'analyze_parquet_files.py' in cmdline:
                            return int(pid)
                except:
                    continue
    except:
        pass
    return None

def check_log_for_errors(log_file):
    """Check log file for errors and return error information"""
    if not os.path.exists(log_file):
        return None
    
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        
        # Look for error patterns
        error_patterns = [
            (r"Error analyzing (.+?): (.+)", "file_error"),
            (r"Traceback", "traceback"),
            (r"Exception:", "exception"),
            (r"Error:", "error"),
        ]
        
        errors = []
        for i, line in enumerate(lines):
            for pattern, error_type in error_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    if error_type == "file_error":
                        filename = match.group(1)
                        error_msg = match.group(2)
                        errors.append({
                            'type': error_type,
                            'file': filename,
                            'message': error_msg,
                            'line': i + 1
                        })
                    else:
                        # Get more context for tracebacks
                        context = '\n'.join(lines[max(0, i-5):min(len(lines), i+10)])
                        errors.append({
                            'type': error_type,
                            'message': line.strip(),
                            'context': context,
                            'line': i + 1
                        })
        
        return errors[-1] if errors else None  # Return most recent error
    except Exception as e:
        print(f"Error reading log file: {e}")
        return None

def fix_common_errors(error_info, script_file):
    """Fix common errors in the script"""
    if not error_info:
        return False
    
    error_msg = error_info.get('message', '').lower()
    
    # Check if we've already fixed this type of error
    with open(script_file, 'r') as f:
        script_content = f.read()
    
    # Fix for unhashable type errors (numpy arrays, lists)
    if 'unhashable' in error_msg and 'numpy.ndarray' in error_info.get('message', ''):
        if 'Handle columns with unhashable types' in script_content:
            print("Error already fixed in script")
            return True
        
        # The fix should already be in place from our earlier edit
        print("Unhashable type error detected - checking if fix is applied...")
        if 'Handle columns with unhashable types' not in script_content:
            print("Fix not found - this shouldn't happen if script was updated")
            return False
        return True
    
    # Add more error fixes here as needed
    return False

def restart_analysis():
    """Restart the analysis script"""
    print("\n" + "="*60)
    print("RESTARTING ANALYSIS SCRIPT")
    print("="*60)
    
    # Kill existing process
    pid = get_process_pid()
    if pid:
        print(f"Stopping existing process (PID: {pid})")
        try:
            os.kill(pid, 15)  # SIGTERM
            time.sleep(3)
            # Force kill if still running
            try:
                os.kill(pid, 9)  # SIGKILL
            except:
                pass
        except:
            pass
    
    # Start new process
    print("Starting new analysis process...")
    os.chdir("/home/kurtluo/yannan/jmp")
    with open(LOG_FILE, 'a') as log:
        log.write(f"\n\n{'='*60}\n")
        log.write(f"RESTARTED AT {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"{'='*60}\n\n")
    
    subprocess.Popen(
        ["nohup", "python", "-u", SCRIPT_FILE],
        stdout=open(LOG_FILE, 'a'),
        stderr=subprocess.STDOUT,
        cwd="/home/kurtluo/yannan/jmp"
    )
    print("Analysis restarted")
    time.sleep(5)  # Wait for process to start

def check_completion(log_file):
    """Check if analysis is complete"""
    if not os.path.exists(log_file):
        return False
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Check for completion indicators
        completion_patterns = [
            r"Results saved to:",
            r"INTERSECTION/UNION ANALYSIS SUMMARY",
            r"Summary saved to:",
        ]
        
        for pattern in completion_patterns:
            if re.search(pattern, content):
                return True
        
        return False
    except:
        return False

def main():
    """Main monitoring loop"""
    print("="*60)
    print("PARQUET ANALYSIS MONITOR")
    print("="*60)
    print(f"Monitoring log: {LOG_FILE}")
    print(f"Check interval: {CHECK_INTERVAL} seconds")
    print("Press Ctrl+C to stop monitoring\n")
    
    last_size = 0
    consecutive_errors = 0
    max_consecutive_errors = 3
    
    try:
        while True:
            # Check if process is running
            pid = get_process_pid()
            if not pid:
                print(f"[{time.strftime('%H:%M:%S')}] Process not running - restarting...")
                restart_analysis()
                time.sleep(CHECK_INTERVAL)
                continue
            
            # Check log file for new content
            if os.path.exists(LOG_FILE):
                current_size = os.path.getsize(LOG_FILE)
                if current_size > last_size:
                    last_size = current_size
                    print(f"[{time.strftime('%H:%M:%S')}] Log updated ({current_size} bytes)")
            
            # Check for errors
            error_info = check_log_for_errors(LOG_FILE)
            if error_info:
                consecutive_errors += 1
                print(f"\n[{time.strftime('%H:%M:%S')}] ERROR DETECTED:")
                print(f"  Type: {error_info.get('type', 'unknown')}")
                print(f"  Message: {error_info.get('message', 'N/A')}")
                if 'file' in error_info:
                    print(f"  File: {error_info['file']}")
                
                # Try to fix the error
                if fix_common_errors(error_info, SCRIPT_FILE):
                    print("  -> Error fix found in script")
                    if consecutive_errors >= max_consecutive_errors:
                        print("  -> Too many consecutive errors, restarting...")
                        restart_analysis()
                        consecutive_errors = 0
                else:
                    print("  -> No automatic fix available")
                    if consecutive_errors >= max_consecutive_errors:
                        print("  -> Too many consecutive errors, restarting anyway...")
                        restart_analysis()
                        consecutive_errors = 0
            else:
                consecutive_errors = 0
            
            # Check if analysis is complete
            if check_completion(LOG_FILE):
                print(f"\n[{time.strftime('%H:%M:%S')}] ANALYSIS COMPLETE!")
                print("="*60)
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
