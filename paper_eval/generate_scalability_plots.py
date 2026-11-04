#!/usr/bin/env python3
"""
Generate Scalability Plots and Summary Tables for SpatialBench.
Evaluates strong scaling across 1, 2, 4, 8, 16 nodes for:
  - Apache Sedona Cluster
  - Trino Cluster
  - LocationTech GeoMesa
  - ClickHouse Cluster
"""

import json
import os
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

ENGINES = {
    "sedona": "Apache Sedona",
    "trino": "Trino Cluster",
    "geomesa": "LocationTech GeoMesa",
    "clickhouse": "ClickHouse Cluster",
}

COLORS = {
    "sedona": "#2b5c8f",
    "trino": "#e6550d",
    "geomesa": "#31a354",
    "clickhouse": "#756bb1",
}

MARKERS = {
    "sedona": "o",
    "trino": "s",
    "geomesa": "^",
    "clickhouse": "D",
}

def load_data():
    data = {}
    nodes = [1, 2, 4, 8, 16]
    for eng in ENGINES:
        data[eng] = {}
        for n in nodes:
            fpath = RESULTS_DIR / f"{eng}_cluster_sf100_nodes_{n}.json"
            if fpath.exists():
                with open(fpath) as f:
                    q_list = json.load(f)
                data[eng][n] = {q["query"]: q.get("mean") for q in q_list if q.get("status") == "SUCCESS" and q.get("mean") is not None}
            else:
                data[eng][n] = {}
    return data

def compute_scaling_metrics(data):
    # Find queries that succeeded across ALL tested node counts for that engine
    summary = {}
    for eng, eng_name in ENGINES.items():
        all_nodes = [1, 2, 4, 8, 16]
        available_nodes = [n for n in all_nodes if n in data[eng] and len(data[eng][n]) > 0]
        if not available_nodes:
            continue
        
        # Consistent queries present across all available nodes
        common_queries = set(data[eng][available_nodes[0]].keys())
        for n in available_nodes[1:]:
            common_queries &= set(data[eng][n].keys())
        common_queries = sorted(list(common_queries))

        total_times = {}
        speedups = {}
        t_base = None
        min_node = min(available_nodes)
        for n in available_nodes:
            tot = sum(data[eng][n][q] for q in common_queries)
            total_times[n] = tot
            if n == min_node:
                t_base = tot

        for n in available_nodes:
            # Normalized speedup relative to min_node (e.g. 1 node)
            speedups[n] = (t_base / total_times[n]) * min_node if total_times[n] > 0 else 0

        summary[eng] = {
            "name": eng_name,
            "available_nodes": available_nodes,
            "common_queries": common_queries,
            "total_times": total_times,
            "speedups": speedups,
        }
    return summary

def plot_speedup(summary):
    fig, ax = plt.subplots(figsize=(8, 6), dpi=300)
    nodes = [1, 2, 4, 8, 16]

    # Ideal linear scaling line
    ax.plot(nodes, nodes, label="Ideal Linear (1.0x)", color="#888888", linestyle="--", linewidth=1.8, zorder=1)

    for eng, s in summary.items():
        ns = s["available_nodes"]
        sp = [s["speedups"][n] for n in ns]
        label = f"{s['name']} ({len(s['common_queries'])} Qs)"
        ax.plot(ns, sp, label=label, color=COLORS[eng], marker=MARKERS[eng], markersize=8, linewidth=2.2, zorder=3)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)
    ax.set_xticks(nodes)
    ax.set_xticklabels([str(n) for n in nodes])
    ax.set_yticks(nodes)
    ax.set_yticklabels([f"{n}x" for n in nodes])

    ax.set_xlabel("Number of Cluster Nodes", fontsize=12, fontweight="bold")
    ax.set_ylabel("Speedup Factor (relative to 1 node)", fontsize=12, fontweight="bold")
    ax.set_title("SpatialBench SF100 Distributed Scalability (Strong Scaling)", fontsize=13, fontweight="bold", pad=12)
    ax.grid(True, which="both", linestyle=":", alpha=0.5)
    ax.legend(frameon=True, facecolor="white", edgecolor="#cccccc", fontsize=10, loc="upper left")

    plt.tight_layout()
    png_path = PLOTS_DIR / "scalability_speedup.png"
    pdf_path = PLOTS_DIR / "scalability_speedup.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved speedup plot to {png_path} and {pdf_path}")

def plot_latency(summary):
    fig, ax = plt.subplots(figsize=(8, 6), dpi=300)
    nodes = [1, 2, 4, 8, 16]

    for eng, s in summary.items():
        ns = s["available_nodes"]
        lat = [s["total_times"][n] for n in ns]
        label = f"{s['name']}"
        ax.plot(ns, lat, label=label, color=COLORS[eng], marker=MARKERS[eng], markersize=8, linewidth=2.2, zorder=3)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=10)
    ax.set_xticks(nodes)
    ax.set_xticklabels([str(n) for n in nodes])

    ax.set_xlabel("Number of Cluster Nodes", fontsize=12, fontweight="bold")
    ax.set_ylabel("Total Execution Time (Seconds, Log Scale)", fontsize=12, fontweight="bold")
    ax.set_title("Total Runtime vs Cluster Size (SF100 Common Queries)", fontsize=13, fontweight="bold", pad=12)
    ax.grid(True, which="both", linestyle=":", alpha=0.5)
    ax.legend(frameon=True, facecolor="white", edgecolor="#cccccc", fontsize=10, loc="upper right")

    plt.tight_layout()
    png_path = PLOTS_DIR / "scalability_latency.png"
    pdf_path = PLOTS_DIR / "scalability_latency.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved latency plot to {png_path} and {pdf_path}")

def generate_markdown_summary(summary):
    lines = [
        "# SpatialBench SF100 Distributed Cluster Scalability Report",
        "",
        "## 1. Strong Scaling Summary Table",
        "",
        "| Engine | Common Queries | 1 Node | 2 Nodes | 4 Nodes | 8 Nodes | 16 Nodes | 16-Node Speedup | Parallel Efficiency |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for eng, s in summary.items():
        q_count = len(s["common_queries"])
        q_str = f"{q_count} Qs ({', '.join(s['common_queries'])})"
        ns = s["available_nodes"]
        times_str = []
        for n in [1, 2, 4, 8, 16]:
            if n in s["total_times"]:
                times_str.append(f"{s['total_times'][n]:.2f}s")
            else:
                times_str.append("N/A")
        
        sp16 = f"{s['speedups'].get(16, 0):.2f}x" if 16 in s["speedups"] else "N/A"
        eff16 = f"{(s['speedups'].get(16, 0) / 16 * 100):.1f}%" if 16 in s["speedups"] else "N/A"
        lines.append(f"| **{s['name']}** | {q_count} Qs | {times_str[0]} | {times_str[1]} | {times_str[2]} | {times_str[3]} | {times_str[4]} | {sp16} | {eff16} |")

    lines.extend([
        "",
        "## 2. Speedup Curve",
        "",
        "![Scalability Speedup](scalability_speedup.png)",
        "",
        "## 3. Total Latency Scaling Curve",
        "",
        "![Scalability Latency](scalability_latency.png)",
        "",
    ])

    report_path = PLOTS_DIR / "scalability_summary.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    print(f"Saved markdown summary to {report_path}")

def main():
    data = load_data()
    summary = compute_scaling_metrics(data)
    if not summary:
        print("No scalability results found yet.")
        return
    plot_speedup(summary)
    plot_latency(summary)
    generate_markdown_summary(summary)

if __name__ == "__main__":
    main()
