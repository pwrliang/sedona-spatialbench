#!/usr/bin/env python3
"""
Thrust 2: Spatial Data Skew and Partitioning Robustness.
Evaluates partitioning algorithms (Uniform Grid, Quadtree, KDB-tree, Morton Z-curve, H3)
under controlled spatial skew: Uniform, Gaussian Mixture, Zipfian, and Real-World.
"""

import json
import os
import time
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

class QuadTreeNode:
    def __init__(self, bbox, depth=0, max_depth=6, capacity=1000):
        self.bbox = bbox # (minx, miny, maxx, maxy)
        self.depth = depth
        self.max_depth = max_depth
        self.capacity = capacity
        self.children = None
        self.points_count = 0

    def insert(self, n_points, xs, ys):
        self.points_count += n_points
        if self.depth >= self.max_depth or n_points <= self.capacity:
            return
        minx, miny, maxx, maxy = self.bbox
        midx = (minx + maxx) / 2.0
        midy = (miny + maxy) / 2.0
        
        q1 = (xs >= midx) & (ys >= midy)
        q2 = (xs < midx) & (ys >= midy)
        q3 = (xs < midx) & (ys < midy)
        q4 = (xs >= midx) & (ys < midy)

        self.children = [
            QuadTreeNode((midx, midy, maxx, maxy), self.depth + 1, self.max_depth, self.capacity),
            QuadTreeNode((minx, midy, midx, maxy), self.depth + 1, self.max_depth, self.capacity),
            QuadTreeNode((minx, miny, midx, midy), self.depth + 1, self.max_depth, self.capacity),
            QuadTreeNode((midx, miny, maxx, midy), self.depth + 1, self.max_depth, self.capacity),
        ]
        if np.any(q1): self.children[0].insert(np.sum(q1), xs[q1], ys[q1])
        if np.any(q2): self.children[1].insert(np.sum(q2), xs[q2], ys[q2])
        if np.any(q3): self.children[2].insert(np.sum(q3), xs[q3], ys[q3])
        if np.any(q4): self.children[3].insert(np.sum(q4), xs[q4], ys[q4])

    def get_leaf_counts(self):
        if self.children is None:
            return [self.points_count]
        counts = []
        for c in self.children:
            counts.extend(c.get_leaf_counts())
        return counts

def partition_kdtree(xs, ys, depth=0, max_depth=6):
    if depth >= max_depth or len(xs) == 0:
        return [len(xs)]
    axis = depth % 2
    if axis == 0:
        med = np.median(xs)
        left = xs < med
        right = ~left
    else:
        med = np.median(ys)
        left = ys < med
        right = ~left
    return partition_kdtree(xs[left], ys[left], depth + 1, max_depth) + \
           partition_kdtree(xs[right], ys[right], depth + 1, max_depth)

def partition_uniform_grid(xs, ys, minx, miny, maxx, maxy, grid_dim=8):
    gx = np.clip(((xs - minx) / (maxx - minx) * grid_dim).astype(int), 0, grid_dim - 1)
    gy = np.clip(((ys - miny) / (maxy - miny) * grid_dim).astype(int), 0, grid_dim - 1)
    cell_idx = gx * grid_dim + gy
    counts = np.bincount(cell_idx, minlength=grid_dim * grid_dim)
    return counts.tolist()

def partition_morton_zcurve(xs, ys, minx, miny, maxx, maxy, num_partitions=64):
    nx = np.clip(((xs - minx) / (maxx - minx) * 1024).astype(np.uint32), 0, 1023)
    ny = np.clip(((ys - miny) / (maxy - miny) * 1024).astype(np.uint32), 0, 1023)
    # Interleave bits
    def part1by1(n):
        n = (n | (n << 8)) & 0x00FF00FF
        n = (n | (n << 4)) & 0x0F0F0F0F
        n = (n | (n << 2)) & 0x33333333
        n = (n | (n << 1)) & 0x55555555
        return n
    z = (part1by1(ny) << 1) | part1by1(nx)
    # Range partitioned into 64 equal-width Z-ranges
    z_min, z_max = 0, 1 << 20
    p_idx = np.clip((z / z_max * num_partitions).astype(int), 0, num_partitions - 1)
    counts = np.bincount(p_idx, minlength=num_partitions)
    return counts.tolist()

def generate_datasets(N=2_000_000):
    np.random.seed(42)
    minx, miny, maxx, maxy = -112.5, 33.5, -111.0, 35.5
    datasets = {}

    # 1. Uniform
    print(f"Generating Uniform Random dataset ({N:,} points)...")
    u_x = np.random.uniform(minx, maxx, N)
    u_y = np.random.uniform(miny, maxy, N)
    datasets["Uniform (Baseline)"] = (u_x, u_y)

    # 2. Clustered (Gaussian Mixture: 10 clusters)
    print("Generating Gaussian Clustered dataset...")
    n_clusters = 10
    cx = np.random.uniform(minx + 0.2, maxx - 0.2, n_clusters)
    cy = np.random.uniform(miny + 0.2, maxy - 0.2, n_clusters)
    weights = np.random.dirichlet(np.ones(n_clusters))
    counts = np.random.multinomial(N, weights)
    g_xs, g_ys = [], []
    for k in range(n_clusters):
        g_xs.append(np.random.normal(cx[k], 0.03, counts[k]))
        g_ys.append(np.random.normal(cy[k], 0.03, counts[k]))
    g_x = np.clip(np.concatenate(g_xs), minx, maxx)
    g_y = np.clip(np.concatenate(g_ys), miny, maxy)
    datasets["Gaussian Clustered (10 Hotspots)"] = (g_x, g_y)

    # 3. Zipfian Skew (Power Law alpha = 1.5)
    print("Generating Zipfian Power-Law dataset...")
    n_bins = 100
    ranks = np.arange(1, n_bins + 1)
    zipf_weights = 1.0 / (ranks ** 1.5)
    zipf_weights /= zipf_weights.sum()
    b_counts = np.random.multinomial(N, zipf_weights)
    z_xs, z_ys = [], []
    # Assign each bin to a localized grid tile
    bx = np.linspace(minx, maxx, 11)
    by = np.linspace(miny, maxy, 11)
    idx = 0
    for i in range(10):
        for j in range(10):
            c = b_counts[idx]
            z_xs.append(np.random.uniform(bx[i], bx[i+1], c))
            z_ys.append(np.random.uniform(by[j], by[j+1], c))
            idx += 1
    datasets["Zipfian Power-Law (α=1.5)"] = (np.concatenate(z_xs), np.concatenate(z_ys))

    # 4. Real-world sample (from SpatialBench SF1 or SF10)
    sf1_trip = Path("paper_eval/benchmark-data-sf1/trip")
    if sf1_trip.exists():
        print("Sampling real-world trip points from benchmark-data-sf1...")
        import duckdb
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            df = con.execute(f"SELECT ST_X(ST_GeomFromWKB(t_pickuploc)) as x, ST_Y(ST_GeomFromWKB(t_pickuploc)) as y FROM '{sf1_trip}/*.parquet' USING SAMPLE {min(N, 1_000_000)}").df()
            datasets["Empirical Real-World (SF1)"] = (df["x"].values, df["y"].values)
        except Exception:
            # Fallback using pyarrow and binary unpacking
            import pyarrow.parquet as pq, struct
            tbl = pq.read_table(str(list(sf1_trip.glob('*.parquet'))[0]), columns=['t_pickuploc'])
            wkb_list = tbl['t_pickuploc'].to_pylist()[:min(N, 500_000)]
            xs = [struct.unpack('<d', w[9:17])[0] for w in wkb_list if len(w) >= 21]
            ys = [struct.unpack('<d', w[17:25])[0] for w in wkb_list if len(w) >= 21]
            datasets["Empirical Real-World (SF1)"] = (np.array(xs), np.array(ys))
        con.close()

    return datasets, (minx, miny, maxx, maxy)

def evaluate_skew():
    datasets, (minx, miny, maxx, maxy) = generate_datasets(N=2_000_000)
    partitioners = ["Uniform Grid", "Quadtree", "KDB-Tree", "Morton Z-Curve"]
    results = {}

    for d_name, (xs, ys) in datasets.items():
        results[d_name] = {}
        # Uniform Grid (8x8 = 64 cells)
        ug_counts = np.array(partition_uniform_grid(xs, ys, minx, miny, maxx, maxy, grid_dim=8))
        
        # Quadtree (max_depth=3 -> up to 64 leaves)
        qt = QuadTreeNode((minx, miny, maxx, maxy), depth=0, max_depth=3, capacity=len(xs)//64)
        qt.insert(len(xs), xs, ys)
        qt_counts = np.array(qt.get_leaf_counts())

        # KDB-tree (depth=6 -> 64 balanced leaves)
        kd_counts = np.array(partition_kdtree(xs, ys, depth=0, max_depth=6))

        # Morton Z-curve
        mz_counts = np.array(partition_morton_zcurve(xs, ys, minx, miny, maxx, maxy, num_partitions=64))

        for p_name, counts in [
            ("Uniform Grid", ug_counts),
            ("Quadtree", qt_counts),
            ("KDB-Tree", kd_counts),
            ("Morton Z-Curve", mz_counts),
        ]:
            mu = np.mean(counts)
            sigma = np.std(counts)
            cv = sigma / mu if mu > 0 else 0
            straggler = np.max(counts) / mu if mu > 0 else 0
            results[d_name][p_name] = {
                "cv": round(float(cv), 4),
                "straggler_ratio": round(float(straggler), 4),
                "max_partition": int(np.max(counts)),
                "min_partition": int(np.min(counts)),
                "num_partitions": len(counts),
            }
            print(f"[{d_name}] {p_name:15s} -> CV: {cv:.3f}, Straggler Ratio: {straggler:.2f}x (Max: {np.max(counts):,}, Min: {np.min(counts):,})")

    # Save JSON
    with open(RESULTS_DIR / "spatial_skew_results.json", "w") as f:
        json.dump(results, f, indent=2)

    # Plot
    plot_skew_results(results)

def plot_skew_results(results):
    dist_names = list(results.keys())
    partitioners = ["Uniform Grid", "Quadtree", "KDB-Tree", "Morton Z-Curve"]
    colors = ["#d73027", "#fc8d59", "#4575b4", "#91bfdb"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5), dpi=300)
    x = np.arange(len(dist_names))
    width = 0.2

    # Plot 1: CV (Coefficient of Variation)
    for idx, (p_name, color) in enumerate(zip(partitioners, colors)):
        cvs = [results[d][p_name]["cv"] for d in dist_names]
        ax1.bar(x + idx * width, cvs, width, label=p_name, color=color, edgecolor="black", alpha=0.9)

    ax1.set_ylabel("Coefficient of Variation ($CV = \\sigma / \\mu$)", fontsize=11, fontweight="bold")
    ax1.set_title("Partition Balance Under Spatial Skew (Lower is Better)", fontsize=12, fontweight="bold")
    ax1.set_xticks(x + width * 1.5)
    ax1.set_xticklabels(dist_names, rotation=15, ha="right", fontsize=9)
    ax1.grid(axis="y", linestyle=":", alpha=0.6)
    ax1.legend(frameon=True, facecolor="white")

    # Plot 2: Straggler Ratio (Max / Mean)
    for idx, (p_name, color) in enumerate(zip(partitioners, colors)):
        srs = [results[d][p_name]["straggler_ratio"] for d in dist_names]
        ax2.bar(x + idx * width, srs, width, label=p_name, color=color, edgecolor="black", alpha=0.9)

    ax2.set_ylabel("Straggler Ratio (Max Partition / Mean)", fontsize=11, fontweight="bold")
    ax2.set_title("Partition Straggler Severity (Lower is Better)", fontsize=12, fontweight="bold")
    ax2.set_xticks(x + width * 1.5)
    ax2.set_xticklabels(dist_names, rotation=15, ha="right", fontsize=9)
    ax2.grid(axis="y", linestyle=":", alpha=0.6)
    ax2.legend(frameon=True, facecolor="white")

    plt.tight_layout()
    png_path = PLOTS_DIR / "spatial_skew_analysis.png"
    pdf_path = PLOTS_DIR / "spatial_skew_analysis.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved Skew Analysis plots to {png_path} and {pdf_path}")

if __name__ == "__main__":
    evaluate_skew()
