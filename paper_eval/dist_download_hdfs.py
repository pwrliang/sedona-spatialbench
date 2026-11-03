#!/opt/sedona-venv/bin/python3
import subprocess
import sys
import time
from huggingface_hub import HfApi

REPO_ID = "apache-sedona/spatialbench"
PREFIX = "v0.1.0/sf100/"

api = HfApi()
all_files = [f for f in api.list_repo_files(repo_id=REPO_ID, repo_type="dataset") if f.startswith(PREFIX)]

# Filter for zone and trip
target_files = []
for f in all_files:
    rel_path = f[len(PREFIX):]
    parts = rel_path.split("/")
    if len(parts) == 2:
        tbl, filename = parts
        if tbl in ("zone", "trip"):
            target_files.append((tbl, filename))

print(f"Total target files in HF: {len(target_files)}")

# Check what is already in HDFS
res = subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-ls", "-R", "/data/sf100"], stdout=subprocess.PIPE, text=True)
existing_hdfs_files = set()
for line in res.stdout.splitlines():
    parts = line.split()
    if len(parts) >= 8:
        path = parts[-1]
        existing_hdfs_files.add(path)

# Filter missing files
missing_files = []
for tbl, filename in target_files:
    hdfs_path = f"/data/sf100/{tbl}/{filename}"
    if hdfs_path not in existing_hdfs_files:
        missing_files.append((tbl, filename))

print(f"Missing files to download: {len(missing_files)}")

# 16 workers
workers = [f"worker-{i:02d}" for i in range(1, 17)]

# Distribute missing files evenly across workers
worker_assignments = {w: [] for w in workers}
for idx, item in enumerate(missing_files):
    w = workers[idx % len(workers)]
    worker_assignments[w].append(item)

# Ensure dirs exist
subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-mkdir", "-p", "/data/sf100/zone", "/data/sf100/trip"], check=False)

# Build worker bash script to run in parallel on each worker
for w, items in worker_assignments.items():
    print(f"Worker {w} assigned {len(items)} files")
    cmd_list = []
    for tbl, fn in items:
        url = f"https://huggingface.co/datasets/{REPO_ID}/resolve/main/{PREFIX}{tbl}/{fn}"
        hdfs_target = f"/data/sf100/{tbl}/{fn}"
        cmd_list.append(f'curl -s -L "{url}" | /opt/hadoop/bin/hdfs dfs -put - "{hdfs_target}"')
    
    script_content = "#!/bin/bash\n" + "\n".join(cmd_list) + "\n"
    p = subprocess.Popen(["ssh", w, "cat > /home/ubuntu/dl_batch.sh && chmod +x /home/ubuntu/dl_batch.sh"], stdin=subprocess.PIPE, text=True)
    p.communicate(input=script_content)

print("Starting parallel download execution across all 16 workers...")
for w in workers:
    cmd = "nohup /home/ubuntu/dl_batch.sh > /home/ubuntu/dl_batch.log 2>&1 &"
    subprocess.run(["ssh", w, cmd])

print("All worker download jobs launched.")
