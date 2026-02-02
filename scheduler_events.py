#!/usr/bin/env python3
# scheduler_events.py
"""
Scheduler - Runs events ETL every 5 minutes
"""

import time
import subprocess
import sys
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}", flush=True)

def run_etl():
    log("="*70)
    log("Running Events ETL...")
    
    try:
        result = subprocess.run(
            [sys.executable, 'scripts/etl_events_main.py'],
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            log("✓ ETL completed successfully")
        else:
            log(f"✗ ETL failed with code {result.returncode}")
            
    except Exception as e:
        log(f"✗ Error: {e}")
    
    log("="*70)

if __name__ == "__main__":
    log("="*70)
    log("EVENTS ETL SCHEDULER - Every 5 minutes")
    log("Press Ctrl+C to stop")
    log("="*70)
    
    # Run immediately
    run_etl()
    
    # Then every 5 minutes
    try:
        while True:
            next_run = datetime.fromtimestamp(datetime.now().timestamp() + 300)
            log(f"Next run at {next_run.strftime('%H:%M:%S')}")
            time.sleep(300)
            run_etl()
    except KeyboardInterrupt:
        log("Scheduler stopped by user")
        sys.exit(0)
