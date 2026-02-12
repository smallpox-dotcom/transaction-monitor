import subprocess
import sys
import os
import time
import threading
import requests
import random
from datetime import datetime

def print_header(text):
    print("\n" + "=" * 70)
    print(f" {text}")
    print("=" * 70)

def install_dependencies():
    print_header("INSTALLING DEPENDENCIES")
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt', '--quiet'])
    print(" Dependencies installed")

def load_csv_data():
    print_header("LOADING CSV DATA")
    result = subprocess.run([sys.executable, 'scripts/load_transactions.py'], 
                          capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(" Failed to load CSV data")
        return False
    print(" CSV data loaded successfully")
    return True

def run_api():
    print_header("STARTING API SERVER")
    api_process = subprocess.Popen(
        [sys.executable, 'src/api/transaction_api.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    time.sleep(3)
    print(" API server running on http://localhost:5000")
    return api_process

def run_dashboard():
    print_header("STARTING DASHBOARD")
    dashboard_process = subprocess.Popen(
        [sys.executable, '-m', 'streamlit', 'run', 'src/visualization/dashboard.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    time.sleep(4)
    print(" Dashboard running on http://localhost:8501")
    return dashboard_process

def training_phase():
    print_header("TRAINING MODE - 30 SECONDS")
    print(" Sending approved transactions - NO ALERTS GENERATED")
    print(" System learning normal behavior...\n")
    
    url = "http://localhost:5000/api/transaction"
    start_time = time.time()
    count = 0
    
    try:
        requests.post("http://localhost:5000/api/reset", timeout=2)
    except:
        pass
    
    while time.time() - start_time < 30:
        tx = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "approved"
        }
        try:
            requests.post(url, json=tx, timeout=0.2)
            count += 1
            if count % 20 == 0:
                print(f"   Training: {count} approved transactions sent")
        except:
            pass
        time.sleep(0.1)
    
    print(f"\n Training complete - {count} transactions processed")
    print(" System ready for normal operation\n")
    time.sleep(1)

def random_spike():
    spike_type = random.choice(['failed', 'denied', 'reversed', 'volume'])
    
    if spike_type == 'failed':
        count = random.randint(25, 40)
        print(f"\n   INJECTING FAILED SPIKE - {count} transactions")
        status = 'failed'
    elif spike_type == 'denied':
        count = random.randint(20, 30)
        print(f"\n   INJECTING DENIED SPIKE - {count} transactions")
        status = 'denied'
    elif spike_type == 'reversed':
        count = random.randint(15, 25)
        print(f"\n   INJECTING REVERSED SPIKE - {count} transactions")
        status = 'reversed'
    else:
        count = random.randint(100, 150)
        print(f"\n   INJECTING VOLUME SPIKE - {count} transactions")
        status = 'approved'
    
    url = "http://localhost:5000/api/transaction"
    for i in range(count):
        tx = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status
        }
        try:
            requests.post(url, json=tx, timeout=0.1)
        except:
            pass
        time.sleep(0.02)
    
    print(f"   Spike completed - Check Alerts tab")
    time.sleep(1)

def spike_phase():
    print_header("SPIKE GENERATION - CONTINUOUS")
    print(" Injecting random spikes every 15 seconds")
    print(" Check Alerts tab for generated alerts\n")
    
    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n--- SPIKE CYCLE {cycle} ---")
            random_spike()
            print("\n   Waiting 15 seconds for next spike...")
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n Spike generation stopped")

def cleanup(api_process, dashboard_process, spike_thread):
    print_header("CLEANING UP")
    if spike_thread and spike_thread.is_alive():
        print(" Stopping spike generator...")
    if dashboard_process:
        dashboard_process.terminate()
        print(" Dashboard stopped")
    if api_process:
        api_process.terminate()
        print(" API server stopped")
    print(" Cleanup completed")

def main():
    print_header("TRANSACTION MONITORING SYSTEM - FULL PIPELINE")
    
    install_dependencies()
    
    if not load_csv_data():
        print(" Pipeline failed at data loading")
        return
    
    api_process = run_api()
    
    time.sleep(2)
    
    try:
        requests.get("http://localhost:5000/health", timeout=2)
        print(" API health check passed")
    except:
        print(" API health check failed")
        cleanup(api_process, None, None)
        return
    
    dashboard_process = run_dashboard()
    
    training_phase()
    
    print_header("SYSTEM READY - STARTING SPIKE GENERATION")
    print(" API: http://localhost:5000")
    print(" Dashboard: http://localhost:8501")
    print("\n Spikes will be injected every 15 seconds")
    print("\n Press Ctrl+C to stop all services\n")
    
    spike_thread = threading.Thread(target=spike_phase, daemon=True)
    spike_thread.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n")
        cleanup(api_process, dashboard_process, spike_thread)
        print_header("PIPELINE TERMINATED")
        sys.exit(0)

if __name__ == "__main__":
    main()