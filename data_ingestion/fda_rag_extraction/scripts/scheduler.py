"""
SCHEDULER DAEMON (The Automatic Clock)
--------------------------------------
This script keeps the process alive on your computer.
It doesn't do the heavy lifting; it just waits for the time and wakes up the Manager.

To stop it: Press Ctrl + C in the terminal.
"""

import schedule
import time
import logging
import os
import sys
import subprocess
from datetime import datetime 

# --- CONFIGURATION ---
EXECUTION_TIME = "09:00"  # Local time on your Mac (24h format)

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Clock Logging Configuration (To know it's alive)
logging.basicConfig(
    filename='logs/scheduler_system.log',
    level=logging.INFO,
    format='%(asctime)s - CLOCK - %(message)s',
    filemode='a'  # Append mode
)

def execute_step(script_path, *args):
    """Execute a Python script and return success status"""
    try:
        cmd = [sys.executable, script_path] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(__file__)))
        if result.returncode == 0:
            print(result.stdout)
            return True
        else:
            print(f"❌ Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Failed to execute {script_path}: {e}")
        return False

def scheduled_task():
    """
    Scheduled task: Executes Step 1 (FDA Watcher) only.
    
    IMPORTANT: This only runs Step 1 because:
    - Step 1 may have 403 errors or incomplete data that needs manual review
    - Step 2 should be executed manually after reviewing Step 1 results
    - This ensures data quality and allows for manual intervention when needed
    """
    print(f"\n⏰ RING RING! It's time ({EXECUTION_TIME}). Starting Step 1 (FDA Watcher)...")
    logging.info(f"Starting scheduled execution at {EXECUTION_TIME}")
    
    scripts_dir = os.path.dirname(__file__)
    
    try:
        # Step 1: Scraping (ONLY STEP - requires manual review)
        print(">> Step 1: FDA Watcher (Scraping)...")
        logging.info("Executing Step 1: FDA Watcher")
        step1_success = execute_step(os.path.join(scripts_dir, 'fda_watcher.py'))
        
        if not step1_success:
            print("❌ Step 1 failed. Check logs for details.")
            logging.error("Step 1 (FDA Watcher) failed")
            return
        
        # Check if JSON file was generated
        json_file = None
        project_root = os.path.dirname(scripts_dir)
        for json_file_path in ['data/rag_initial_load.json', 'data/rag_delta_update.json']:
            full_path = os.path.join(project_root, json_file_path)
            if os.path.exists(full_path):
                json_file = json_file_path
                break
        
        if json_file:
            print(f"\n✅ Step 1 completed. JSON file generated: {json_file}")
            print(f"\n📋 NEXT STEPS (Manual):")
            print(f"   1. Review the JSON file: {json_file}")
            print(f"   2. Check for 403 errors or incomplete data")
            print(f"   3. Fix any issues if needed")
            print(f"   4. Then run Step 2 manually:")
            print(f"      python3 scripts/json_split_and_clean.py")
            logging.info(f"Step 1 completed. JSON file: {json_file}")
            logging.info("Waiting for manual execution of Step 2")
        else:
            print("\n✅ Step 1 completed. No new entries found (everything synchronized).")
            logging.info("Step 1 completed. No new entries found.")
        
        print(f"\n💤 Scheduler going back to sleep until tomorrow at {EXECUTION_TIME}...")
        logging.info("Scheduled task completed. Waiting for next execution.")
        
    except Exception as e:
        print(f"❌ Step 1 failed: {e}")
        logging.error(f"Error executing Step 1: {e}", exc_info=True)

def start_clock():
    print("="*60)
    print(f"⏳ AUTOMATION STARTED (Scheduler Mode)")
    print(f"🎯 System will run Step 1 (FDA Watcher) every day at: {EXECUTION_TIME}")
    print(f"")
    print(f"⚠️  IMPORTANT: This scheduler only runs Step 1 (scraping).")
    print(f"   Step 2 must be executed manually after reviewing Step 1 results.")
    print(f"   This ensures data quality and allows for manual intervention when needed.")
    print(f"")
    print(f"📍 Keep this window open (or minimized).")
    print(f"🛑 To exit: Ctrl + C")
    print("="*60)

    # Schedule the daily task
    schedule.every().day.at(EXECUTION_TIME).do(scheduled_task)
    
    # Infinite Loop (The heart that keeps the script alive)
    while True:
        # Check if it's time
        schedule.run_pending()
        # Sleep 1 minute to avoid wasting CPU on your Mac checking every millisecond
        time.sleep(60)

if __name__ == "__main__":
    start_clock()