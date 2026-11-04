#!/usr/bin/env python3
"""
Thrust 3: Storage Format & Metadata Pushdown (GeoParquet vs Parquet vs Spatially Clustered).
Measures I/O bytes read, row groups skipped, and scan latency across:
1. Standard Parquet (Unordered)
2. GeoParquet with Bounding Box Metadata
3. Spatially Z-Ordered Parquet
"""

import json
import os
import struct
import time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
DATA_DIR = Path("paper_eval/data_storage_pushdown")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

def generate_test_tables(N=1_000_000, row_group_size=50_000):
    np.random.seed(42)
    minx, miny, maxx, maxy = -112.5, 33.5, -111.0, 35.5
    
    # Generate points with moderate spatial clustering (Gaussian mixture)
    n_clusters = 8
    cx = np.random.uniform(minx + 0.2, maxx - 0.2, n_clusters)
    cy = np.random.uniform(miny + 0.2, maxy - 0.2, n_clusters)
    weights = np.random.dirichlet(np.ones(n_clusters))
    counts = np.random.multinomial(N, weights)
    xs, ys = [], []
    for k in range(n_clusters):
        xs.append(np.random.normal(cx[k], 0.05, counts[k]))
        ys.append(np.random.normal(cy[k], 0.05, counts[k]))
    xs = np.clip(np.concatenate(xs), minx, maxx)
    ys = np.clip(np.concatenate(ys), miny, maxy)
    
    # Shuffle so standard parquet represents real-world unsorted / time-ordered spatial data
    perm = np.random.permutation(N)
    xs = xs[perm]
    ys = ys[perm]
    
    trip_keys = np.arange(1, N + 1)
    amounts = np.random.uniform(5.0, 150.0, N)
    
    # Create WKB bytes
    wkbs = []
    for x, y in zip(xs, ys):
        # 1 byte byteorder (1=LE), 4 bytes type (1=Point), 8 bytes x, 8 bytes y
        wkbs.append(struct.pack('<BIdd', 1, 1, x, y))

    base_table = pa.Table.from_arrays(
        [pa.array(trip_keys), pa.array(wkbs), pa.array(xs), pa.array(ys), pa.array(amounts)],
        names=['t_tripkey', 't_pickuploc', 'x', 'y', 't_totalamount']
    )

    # 1. Standard Unordered Parquet
    p_std = DATA_DIR / "trips_standard.parquet"
    if not p_std.exists():
        print("Writing trips_standard.parquet...")
        pq.write_table(base_table, p_std, row_group_size=row_group_size, compression="SNAPPY")

    # 2. Spatially Z-Ordered Parquet
    p_zorder = DATA_DIR / "trips_zordered.parquet"
    if not p_zorder.exists():
        print("Writing trips_zordered.parquet (Morton Curve Sorted)...")
        nx = np.clip(((xs - minx) / (maxx - minx) * 1024).astype(np.uint32), 0, 1023)
        ny = np.clip(((ys - miny) / (maxy - miny) * 1024).astype(np.uint32), 0, 1023)
        def part1by1(n):
            n = (n | (n << 8)) & 0x00FF00FF
            n = (n | (n << 4)) & 0x0F0F0F0F
            n = (n | (n << 2)) & 0x33333333
            n = (n | (n << 1)) & 0x55555555
            return n
        z = (part1by1(ny) << 1) | part1by1(nx)
        sort_order = np.argsort(z)
        sorted_table = base_table.take(sort_order)
        pq.write_table(sorted_table, p_zorder, row_group_size=row_group_size, compression="SNAPPY")

    # 3. GeoParquet with embedded bbox metadata
    p_geoparquet = DATA_DIR / "trips_geoparquet.parquet"
    if not p_geoparquet.exists():
        print("Writing trips_geoparquet.parquet with GeoParquet 1.1 metadata...")
        # Add GeoParquet metadata
        geo_meta = {
            "version": "1.1.0",
            "primary_column": "t_pickuploc",
            "columns": {
                "t_pickuploc": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [float(minx), float(miny), float(maxx), float(maxy)],
                }
            }
        }
        existing_meta = base_table.schema.metadata or {}
        new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
        geo_table = base_table.replace_schema_metadata(new_meta)
        pq.write_table(geo_table, p_geoparquet, row_group_size=row_group_size, compression="SNAPPY")

    return p_std, p_geoparquet, p_zorder

def benchmark_pushdown():
    p_std, p_geoparquet, p_zorder = generate_test_tables(N=1_000_000, row_group_size=50_000)
    
    # Spatial filter: Selective bounding box (e.g. 5% of area)
    # Target box: [-112.0, 34.0, -111.7, 34.3]
    q_minx, q_miny, q_maxx, q_maxy = -112.0, 34.0, -111.7, 34.3

    layouts = {
        "Standard Parquet (Unordered)": p_std,
        "GeoParquet 1.1": p_geoparquet,
        "Spatially Z-Ordered Parquet": p_zorder,
    }

    results = {}

    for name, fpath in layouts.items():
        pf = pq.ParquetFile(fpath)
        total_rgs = pf.num_row_groups
        total_bytes = fpath.stat().st_size
        
        # Determine row group pruning via column statistics (min/max of x and y)
        rgs_scanned = 0
        rgs_skipped = 0
        bytes_scanned = 0

        scanned_rg_indices = []
        t0 = time.perf_counter()
        # Evaluate row group stats
        for rg_idx in range(total_rgs):
            rg_meta = pf.metadata.row_group(rg_idx)
            # Find column chunk for x and y
            x_chunk = [rg_meta.column(c) for c in range(rg_meta.num_columns) if rg_meta.column(c).path_in_schema == 'x'][0]
            y_chunk = [rg_meta.column(c) for c in range(rg_meta.num_columns) if rg_meta.column(c).path_in_schema == 'y'][0]
            
            rg_minx = x_chunk.statistics.min
            rg_maxx = x_chunk.statistics.max
            rg_miny = y_chunk.statistics.min
            rg_maxy = y_chunk.statistics.max

            # Disjoint check
            if rg_maxx < q_minx or rg_minx > q_maxx or rg_maxy < q_miny or rg_miny > q_maxy:
                rgs_skipped += 1
            else:
                rgs_scanned += 1
                bytes_scanned += rg_meta.total_byte_size
                scanned_rg_indices.append(rg_idx)

        # Real I/O read of surviving row groups
        if scanned_rg_indices:
            _ = pf.read_row_groups(scanned_rg_indices, columns=['t_tripkey', 't_totalamount', 'x', 'y'])
        scan_time = time.perf_counter() - t0
        pruning_ratio = (rgs_skipped / total_rgs) * 100.0

        results[name] = {
            "total_row_groups": total_rgs,
            "row_groups_skipped": rgs_skipped,
            "row_groups_scanned": rgs_scanned,
            "pruning_ratio_pct": round(pruning_ratio, 2),
            "bytes_scanned_mb": round(bytes_scanned / (1024 * 1024), 2),
            "scan_latency_ms": round(scan_time * 1000, 2),
        }
        print(f"[{name}] -> Skipped {rgs_skipped}/{total_rgs} RGs ({pruning_ratio:.1f}% pruned), Latency: {scan_time*1000:.1f} ms")

    with open(RESULTS_DIR / "storage_pushdown_results.json", "w") as f:
        json.dump(results, f, indent=2)

    plot_pushdown_results(results)

def plot_pushdown_results(results):
    names = list(results.keys())
    prune_pcts = [results[n]["pruning_ratio_pct"] for n in names]
    latencies = [results[n]["scan_latency_ms"] for n in names]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), dpi=300)
    colors = ["#e41a1c", "#377eb8", "#4daf4a"]

    # Pruning Ratio
    ax1.bar(names, prune_pcts, color=colors, edgecolor="black", width=0.55)
    ax1.set_ylabel("Row Groups Pruned (%)", fontsize=11, fontweight="bold")
    ax1.set_title("Storage Metadata Pruning Efficiency (Higher is Better)", fontsize=12, fontweight="bold")
    ax1.set_ylim(0, 100)
    ax1.grid(axis="y", linestyle=":", alpha=0.6)
    for i, v in enumerate(prune_pcts):
        ax1.text(i, v + 2, f"{v:.1f}%", ha="center", fontweight="bold")

    # Scan Latency
    ax2.bar(names, latencies, color=colors, edgecolor="black", width=0.55)
    ax2.set_ylabel("Selective Scan Latency (ms)", fontsize=11, fontweight="bold")
    ax2.set_title("Query Scan Time (Lower is Better)", fontsize=12, fontweight="bold")
    ax2.grid(axis="y", linestyle=":", alpha=0.6)
    for i, v in enumerate(latencies):
        ax2.text(i, v + 2, f"{v:.1f} ms", ha="center", fontweight="bold")

    plt.tight_layout()
    png_path = PLOTS_DIR / "storage_pushdown_comparison.png"
    pdf_path = PLOTS_DIR / "storage_pushdown_comparison.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved Storage Pushdown plots to {png_path} and {pdf_path}")

if __name__ == "__main__":
    benchmark_pushdown()
