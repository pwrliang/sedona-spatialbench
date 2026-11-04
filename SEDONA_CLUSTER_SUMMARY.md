# Apache Sedona Cluster Specification & Operational Guide

## 1. Executive Summary & Cluster Architecture
This document provides full technical context for the distributed **Apache Sedona (v1.9.1)** and **Apache Spark (v3.5.9)** cluster deployed on AWS EC2 across 17 instances (1 Master node + 16 Worker nodes), along with integrated external client machine configuration for **`dl190`**.

- **AWS Region**: `us-east-2` (US East, Ohio)
- **VPC CIDR**: `172.31.0.0/16`
- **Security Group**: `sg-08dcf0b1e99325e75` (`launch-wizard-1`)
  - **Inbound Rule 1**: TCP `22` from `0.0.0.0/0` (external SSH access)
  - **Inbound Rule 2**: All Traffic (`-1`) from `sg-08dcf0b1e99325e75` (internal cluster communication)
- **Total Compute Capacity**: 17 EC2 instances (`c5.2xlarge` equivalent: 8 vCPUs, ~32 GB RAM per worker node).
  - **Master Node**: 8 vCPUs, 32 GB RAM
  - **Worker Nodes**: 16 instances = 128 vCPUs, ~477 GB usable Spark worker memory.

---

## 2. Complete Node Inventory

| Alias / Hostname | Instance ID | Role | Private IP | Public IP | Cores | Memory |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `master` / `liang-spatialbench-master` | `i-002698c50f470a889` | Spark Master | `172.31.0.26` | `3.149.230.198` | 8 | 32 GB |
| `worker-01` | `i-0445259963aa20ba8` | Spark Worker | `172.31.10.184` | `16.59.135.156` | 8 | 30.5 GB |
| `worker-02` | `i-01e14e1b1062497ec` | Spark Worker | `172.31.9.222` | `13.58.233.156` | 8 | 30.5 GB |
| `worker-03` | `i-0dc78ca484f0c0ca0` | Spark Worker | `172.31.14.179` | `3.148.204.104` | 8 | 30.5 GB |
| `worker-04` | `i-02b9f792a396a5beb` | Spark Worker | `172.31.13.178` | `3.148.217.252` | 8 | 30.5 GB |
| `worker-05` | `i-01266f39e69f1eb56` | Spark Worker | `172.31.11.180` | `3.148.243.232` | 8 | 30.5 GB |
| `worker-06` | `i-02acab1c43dc721c6` | Spark Worker | `172.31.13.211` | `3.133.150.200` | 8 | 30.5 GB |
| `worker-07` | `i-02fd44e01d66822b4` | Spark Worker | `172.31.11.10` | `3.148.170.116` | 8 | 30.5 GB |
| `worker-08` | `i-0135e43b8fb2440d5` | Spark Worker | `172.31.1.231` | `18.222.168.8` | 8 | 30.5 GB |
| `worker-09` | `i-046c37d91aa61ab42` | Spark Worker | `172.31.3.78` | `18.226.133.76` | 8 | 30.5 GB |
| `worker-10` | `i-0502ee59edea1ed83` | Spark Worker | `172.31.0.45` | `3.15.6.226` | 8 | 30.5 GB |
| `worker-11` | `i-0b83b6d7b4c794ea6` | Spark Worker | `172.31.2.99` | `18.189.189.72` | 8 | 30.5 GB |
| `worker-12` | `i-0f7cde90706b13dfc` | Spark Worker | `172.31.7.33` | `3.17.181.153` | 8 | 30.5 GB |
| `worker-13` | `i-0ccf5379c01dbada3` | Spark Worker | `172.31.11.70` | `13.59.129.193` | 8 | 30.5 GB |
| `worker-14` | `i-01ea14ea29fa38971` | Spark Worker | `172.31.14.133` | `3.143.3.187` | 8 | 30.5 GB |
| `worker-15` | `i-0da4c5e532813f93a` | Spark Worker | `172.31.12.65` | `3.147.61.206` | 8 | 30.5 GB |
| `worker-16` | `i-0e520e3882f125744` | Spark Worker | `172.31.9.32` | `13.184.164.75` | 8 | 30.5 GB |

---

## 3. Access, Credentials & Networking

- **SSH Private Key (Local)**: `~/.ssh/liang_aws.pem`
- **Default SSH User**: `ubuntu`
- **Sudo Permissions**: Full passwordless `sudo` on all nodes.
- **SSH Command from Local**:
  ```bash
  ssh -i ~/.ssh/liang_aws.pem ubuntu@3.149.230.198
  ```
- **Internal Passwordless SSH Mesh**:
  - The private key is installed on every node at `/home/ubuntu/.ssh/id_rsa` (`0600`).
  - Strict host key checking is disabled in `/home/ubuntu/.ssh/config`.
  - Every node can connect to any other node directly without passwords via hostname (e.g. `ssh worker-03` or `ssh master`).
- **Hosts Resolution (`/etc/hosts`)**:
  Every node's `/etc/hosts` includes the cluster IP-to-hostname mappings (`master`, `worker-01` .. `worker-16`).

---

## 4. Software Stack & File System Layout

All software is identically installed across all 17 instances:

| Component | Version | Installation Path / Details |
| :--- | :--- | :--- |
| **Operating System** | Ubuntu 26.04 LTS (Resolute) | Linux kernel `7.0.0-1014-aws x86_64` |
| **Java** | OpenJDK 17.0.20 | `/usr/lib/jvm/java-17-openjdk-amd64` |
| **Apache Spark** | 3.5.9 (Hadoop 3 bin) | `/opt/spark` (symlink to `/opt/spark-3.5.9-bin-hadoop3`) |
| **Apache Sedona** | **1.9.1** (Latest release) | `sedona-spark-shaded-3.5_2.12-1.9.1.jar` in `/opt/spark/jars/` |
| **GeoTools Wrapper**| **1.9.1-33.5** | `geotools-wrapper-1.9.1-33.5.jar` in `/opt/spark/jars/` |
| **Python Environment**| Python 3.14.4 + Virtualenv | `/opt/sedona-venv` |
| **Python Packages** | `apache-sedona==1.9.1`, `pyspark==3.5.5`, `geopandas==1.2.0`, `shapely==2.2.0`, `pyproj`, `pyogrio` | `/opt/sedona-venv/lib/python3.14/site-packages/` |

---

## 5. Spark & Sedona Configuration Files

### 5.1 Environment Configuration (`/opt/spark/conf/spark-env.sh`)
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export PYSPARK_PYTHON=/opt/sedona-venv/bin/python3
export PYSPARK_DRIVER_PYTHON=/opt/sedona-venv/bin/python3
```

### 5.2 Spark Defaults (`/opt/spark/conf/spark-defaults.conf`)
```properties
spark.master                     spark://master:7077
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator           org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
spark.sql.extensions             org.apache.sedona.sql.SedonaSqlExtensions
spark.jars                       /opt/spark/jars/sedona-spark-shaded-3.5_2.12-1.9.1.jar,/opt/spark/jars/geotools-wrapper-1.9.1-33.5.jar
spark.driver.extraClassPath      /opt/spark/jars/sedona-spark-shaded-3.5_2.12-1.9.1.jar:/opt/spark/jars/geotools-wrapper-1.9.1-33.5.jar
spark.executor.extraClassPath    /opt/spark/jars/sedona-spark-shaded-3.5_2.12-1.9.1.jar:/opt/spark/jars/geotools-wrapper-1.9.1-33.5.jar
```

### 5.3 Workers Roster (`/opt/spark/conf/workers`)
Lists `worker-01` through `worker-16`.

### 5.4 System Profile (`/etc/profile.d/spark.sh`)
Automatically sets `SPARK_HOME=/opt/spark`, `JAVA_HOME`, and prepends `/opt/spark/bin`, `/opt/spark/sbin`, and `/opt/sedona-venv/bin` to `PATH`.

---

## 6. Client Machine Integration: `dl190`

The external development machine **`dl190`** (`cse-dnc196862d.coeit.osu.edu`, user `geng.161`) has been configured to directly operate, monitor, and submit jobs to the Sedona cluster.

### 6.1 Authentication & Configuration on `dl190`
- **Key Installed**: `~/.ssh/liang_aws.pem` (`chmod 600`).
- **Mutual Authorization**: `dl190`'s public key (`~/.ssh/id_rsa.pub`) is added to `/home/ubuntu/.ssh/authorized_keys` on `master`.
- **SSH Config Entry (`~/.ssh/config` on `dl190`)**:
  ```ssh-config
  Host sedona-master liang-spatialbench-master
      HostName 3.149.230.198
      User ubuntu
      IdentityFile ~/.ssh/liang_aws.pem
      IdentityFile ~/.ssh/id_rsa
      StrictHostKeyChecking no
      UserKnownHostsFile /dev/null
      LogLevel ERROR
  ```

### 6.2 The `sedona-cluster` CLI Tool on `dl190`
Located at `~/.local/bin/sedona-cluster` (in system `PATH`):

| Command | Purpose |
| :--- | :--- |
| `sedona-cluster status` | Queries cluster JSON API; displays master status, alive workers (16/16), and total cores/memory. |
| `sedona-cluster ssh` | Opens an interactive terminal session on `master`. |
| `sedona-cluster run <script.py>` | Copies a local Python script from `dl190` to `master` and executes it using `/opt/sedona-venv/bin/python3`. |
| `sedona-cluster sql "<query>"` | Runs an ad-hoc Spark-SQL / Sedona spatial query on the cluster. |
| `sedona-cluster ui` | Sets up an SSH port-forward tunnel (`8080:localhost:8080`) to view the Spark Master Web UI at `http://localhost:8080`. |

### 6.3 Quick Verification from `dl190`
Run from a shell on `dl190`:
```bash
# 1. Check cluster status
sedona-cluster status

# 2. Run a Sedona Python job
sedona-cluster run my_spatial_script.py

# 3. Direct SSH
ssh sedona-master
```

---

## 7. How to Operate & Submit Jobs Directly on Cluster

### 7.1 Cluster Lifecycle
From the master node (`ubuntu@master`):
```bash
# Start all services (Master + all 16 Workers)
/opt/spark/sbin/start-all.sh

# Stop all services
/opt/spark/sbin/stop-all.sh
```

### 7.2 Monitoring & Web UI
- **Spark Master URL**: `spark://master:7077`
- **Web UI URL**: `http://172.31.0.26:8080` (or `http://master:8080`)
- **To view Web UI in local browser**:
  ```bash
  ssh -N -L 8080:localhost:8080 -i ~/.ssh/liang_aws.pem ubuntu@3.149.230.198
  ```
  Then open `http://localhost:8080` in your browser.

### 7.3 Executing PySpark Scripts with Apache Sedona
Always use the virtual environment's Python binary:
```bash
/opt/sedona-venv/bin/python3 /path/to/your_script.py
```

Standard boilerplate for Python scripts:
```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# Initialize Spark with Sedona registered
config = (
    SedonaContext.builder()
    .master("spark://master:7077")
    .appName("MySpatialJob")
    .getOrCreate()
)
spark = SedonaContext.create(config)

# Run standard Sedona SQL functions
df = spark.sql("""
    SELECT ST_Point(lon, lat) AS geom, ST_Buffer(ST_Point(lon, lat), 0.5) AS buffer
    FROM my_table
""")
```

### 7.4 Using Spark-SQL CLI
```bash
spark-sql --master spark://master:7077
```

### 7.5 Using Spark-Submit
```bash
spark-submit \
  --master spark://master:7077 \
  --class org.example.SpatialApp \
  my_app.jar
```

---

## 8. Verified Test Executions

1. **Sedona Geometry Processing**:
   - `ST_Point(-84.3880, 33.7490)` -> `POINT (-84.388 33.749)`
   - `ST_Area(ST_Buffer(..., 1.0))` -> `3.121445152258052`
2. **Distributed Spatial Join**:
   - Tested joining 100,000 spatial points against complex polygon boundaries using `ST_Contains`.
   - Workload was successfully dispatched across **128 parallel tasks** distributed across all 16 workers with zero failures:
     ```text
     +------------+--------------------+
     | region_name|points_inside_region|
     +------------+--------------------+
     |   West_Zone|               10900|
     |Midwest_Zone|               11610|
     +------------+--------------------+
     ```
3. **Remote Dispatch from `dl190`**:
   - Tested `sedona-cluster run ~/test_dl190_sedona.py` from `dl190`.
   - Successfully connected to `spark://master:7077`, executed `ST_Point` / `ST_Buffer` across the cluster, and printed results back to `dl190`.

---

## 9. Guidance & Best Practices for AI Agents Working on this Cluster

1. **Always Target `spark://master:7077`**: The cluster operates in Spark Standalone mode. Never use `local[*]` if you intend to benchmark distributed performance.
2. **Cluster File Transfers**:
   - If distributing input datasets (GeoJSON, WKT, Parquet, CSV), you can scp to `master` and use `rsync -av /path/to/data ubuntu@worker-XX:/path/to/data` across the high-speed VPC link (throughput is ~250–450 MB/s per node).
3. **Python 3.14 Serialization Note**:
   - When building DataFrames in PySpark, use `spark.sql("SELECT ...")` or `spark.read.parquet(...)` / `spark.read.format("geoparquet")` rather than passing large local Python dictionaries into `spark.createDataFrame(python_list)`, which uses standard pickle.
4. **Logs Location**:
   - Master logs: `/opt/spark/logs/spark-ubuntu-org.apache.spark.deploy.master.Master-*.out`
   - Worker logs: `/opt/spark/logs/spark-ubuntu-org.apache.spark.deploy.worker.Worker-*.out` on each worker node.
