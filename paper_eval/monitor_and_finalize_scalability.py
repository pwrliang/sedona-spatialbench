#!/usr/bin/env python3
"""
Monitor Scalability Benchmarks, Collect Results, Generate Plots/Reports, Push to Git, and Power Off AWS Cluster.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

MASTER_HOST = "3.149.230.198"
SSH_KEY = os.path.expanduser("~/.ssh/id_rsa")
REPO_DIR = Path("/home/geng.161/sedona-spatialbench")
RESULTS_DIR = REPO_DIR / "paper_eval" / "results"
PLOTS_DIR = REPO_DIR / "paper_eval" / "plots"
REPORT_MD = REPO_DIR / "paper_eval" / "failure_analysis" / "REPORT_VLDB.md"

def run_ssh(cmd, timeout=30):
    full_cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
        "-i", SSH_KEY, f"ubuntu@{MASTER_HOST}", cmd
    ]
    return subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)

def is_orchestration_running():
    res = run_ssh("ps aux | grep '[o]rchestrate_scalability.py'")
    return res.returncode == 0 and len(res.stdout.strip()) > 0

def start_orchestration():
    print("Launching orchestrate_scalability.py in background on master...")
    res = run_ssh("nohup python3 /home/ubuntu/orchestrate_scalability.py > /home/ubuntu/orchestrate.out 2>&1 &")
    time.sleep(2)
    if is_orchestration_running():
        print("orchestrate_scalability.py successfully started on master!")
        return True
    else:
        print("Failed to start orchestrate_scalability.py. Output:", res.stdout, res.stderr)
        return False

def check_done():
    res = run_ssh("test -f /home/ubuntu/SCALABILITY_DONE.flag && echo 'DONE' || echo 'RUNNING'")
    return "DONE" in res.stdout

def get_latest_log():
    res = run_ssh("tail -n 15 /home/ubuntu/logs/orchestrate_scalability.log 2>/dev/null || true")
    return res.stdout.strip()

def sync_results():
    print("Syncing result JSONs from master to local paper_eval/results/...")
    cmd = [
        "scp", "-o", "StrictHostKeyChecking=no", "-i", SSH_KEY,
        f"ubuntu@{MASTER_HOST}:/home/ubuntu/results/*nodes_*.json",
        str(RESULTS_DIR)
    ]
    subprocess.run(cmd, check=True)
    print("All scalability result JSONs synced successfully.")

def poweroff_cluster():
    print("POWERING OFF ALL 16 WORKER NODES AND MASTER NODE TO PREVENT AWS CHARGES...")
    poweroff_cmd = "for w in $(cat /opt/spark/conf/workers); do echo \"Powering off $w\"; ssh -o StrictHostKeyChecking=no $w 'sudo poweroff' & done; wait; sleep 5; sudo poweroff"
    try:
        run_ssh(poweroff_cmd, timeout=30)
    except Exception as e:
        print("Poweroff command sent (connection terminated as expected):", e)
    print("All cluster nodes powered off successfully!")

def main():
    if not is_orchestration_running() and not check_done():
        if not start_orchestration():
            sys.exit(1)

    print("Monitoring scalability execution. Checking status every 60 seconds...")
    while not check_done():
        running = is_orchestration_running()
        log_snippet = get_latest_log()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Status: {'RUNNING' if running else 'STOPPED'}")
        if log_snippet:
            print("--- Log Snippet ---")
            print(log_snippet)
            print("-------------------")
        if not running and not check_done():
            print("WARNING: Orchestration stopped without DONE flag! Retrying or checking errors...")
            err_log = run_ssh("cat /home/ubuntu/orchestrate.out | tail -n 20").stdout
            print("orchestrate.out tail:", err_log)
            sys.exit(1)
        time.sleep(60)

    print("\n=======================================================")
    print("ALL SCALABILITY EXPERIMENTS FINISHED ON CLUSTER!")
    print("=======================================================\n")

    sync_results()

    # Generate plots
    print("Generating scalability plots...")
    subprocess.run(["python3", str(REPO_DIR / "paper_eval" / "generate_scalability_plots.py")], check=True)

    # Git commit and push before shutdown
    print("Committing and pushing results to Git...")
    subprocess.run(["git", "add", "paper_eval/"], cwd=str(REPO_DIR), check=True)
    subprocess.run(["git", "commit", "-m", "Add distributed scalability experiments (1, 2, 4, 8, 16 nodes) and speedup plots"], cwd=str(REPO_DIR))
    subprocess.run(["git", "push", "origin", "vldb2027-eval"], cwd=str(REPO_DIR), check=True)

    # Power off cluster
    poweroff_cluster()
    print("ALL DONE! AWS CLUSTER POWERED OFF.")

if __name__ == "__main__":
    main()
