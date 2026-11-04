#!/usr/bin/env python3
"""
Generate Weak Scalability Plots for Sedona SpatialBench (PVLDB 2027).
Plots:
1. Query Runtime vs Number of Nodes (Log-Linear) - Ideal is horizontal line
2. Weak Scaling Efficiency (%) vs Number of Nodes - Ideal is 100%
"""

import json
import os
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def plot_weak_scaling():
    sedona_file = RESULTS_DIR / "weak_scalability_sedona.json"
    if not sedona_file.exists():
        print("Waiting for weak_scalability_sedona.json...")
        return

    with open(sedona_file) as f:
        data = json.load(f)

    nodes = [int(n) for n in data.keys()]
    nodes.sort()

    queries = list(data[str(nodes[0])].keys())

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5), dpi=300)

    colors = {
        "q1": "#e41a1c", # Red
        "q3": "#377eb8", # Blue
        "q4": "#4daf4a", # Green
        "q5": "#984ea3", # Purple
        "q6": "#ff7f00", # Orange
        "q7": "#a65628", # Brown
    }
    markers = {
        "q1": "o", "q3": "s", "q4": "^", "q5": "v", "q6": "D", "q7": "P"
    }

    # 1. Runtime vs Nodes
    for q in queries:
        times = [data[str(n)][q] for n in nodes if data[str(n)][q] is not None]
        valid_nodes = [n for n in nodes if data[str(n)][q] is not None]
        if times:
            ax1.plot(valid_nodes, times, marker=markers.get(q, "o"), color=colors.get(q, "black"),
                     linewidth=2.2, markersize=7, label=f"Query {q.upper()}")

    ax1.set_xscale("log", base=2)
    ax1.set_xticks(nodes)
    ax1.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax1.set_xlabel("Worker Node Count (N)", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Query Execution Latency (Seconds)", fontsize=11, fontweight="bold")
    ax1.set_title("(a) Weak Scaling Runtimes (Constant SF=6.25/node)", fontsize=12, fontweight="bold")
    ax1.grid(True, which="both", linestyle=":", alpha=0.6)
    ax1.legend(fontsize=9, loc="upper left")

    # 2. Weak Scaling Efficiency E_weak = (T_1 / T_N) * 100%
    for q in queries:
        t1 = data[str(nodes[0])][q]
        if t1 is not None and t1 > 0:
            effs = []
            valid_nodes = []
            for n in nodes:
                tn = data[str(n)][q]
                if tn is not None and tn > 0:
                    eff = (t1 / tn) * 100.0
                    effs.append(eff)
                    valid_nodes.append(n)
            ax2.plot(valid_nodes, effs, marker=markers.get(q, "o"), color=colors.get(q, "black"),
                     linewidth=2.2, markersize=7, label=f"Query {q.upper()}")

    # Ideal 100% line
    ax2.axhline(100, color="gray", linestyle="--", linewidth=1.8, label="Ideal (100% Linear)")

    ax2.set_xscale("log", base=2)
    ax2.set_xticks(nodes)
    ax2.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax2.set_ylim(0, 125)
    ax2.set_xlabel("Worker Node Count (N)", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Weak Scaling Efficiency (%)", fontsize=11, fontweight="bold")
    ax2.set_title("(b) Weak Scaling Efficiency E_weak", fontsize=12, fontweight="bold")
    ax2.grid(True, which="both", linestyle=":", alpha=0.6)
    ax2.legend(fontsize=9, loc="lower left")

    plt.tight_layout()
    png_path = PLOTS_DIR / "weak_scalability.png"
    pdf_path = PLOTS_DIR / "weak_scalability.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved Weak Scalability plots to {png_path} and {pdf_path}")

if __name__ == "__main__":
    plot_weak_scaling()
