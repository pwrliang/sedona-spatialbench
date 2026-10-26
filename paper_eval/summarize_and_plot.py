#!/usr/bin/env python3
"""
Summarize SpatialBench results and generate publication-quality figures
for Section 5 of the VLDB 2027 paper.
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path
import numpy as np

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


QUERY_NAMES = [f"q{i}" for i in range(1, 13)]

QUERY_CATEGORIES = {
    "Filtering (Q1-Q3)": ["q1", "q2", "q3"],
    "Join Plan (Q4, Q6)": ["q4", "q6"],
    "Geometry (Q5, Q7, Q9)": ["q5", "q7", "q9"],
    "Heavy Joins (Q8, Q10-Q12)": ["q8", "q10", "q11", "q12"]
}

ENGINE_COLORS = {
    "sedonadb": "#1f77b4",       # Blue
    "duckdb": "#ff7f0e",         # Orange
    "spatial_polars": "#2ca02c", # Green
    "geopandas": "#d62728",      # Red
    "apache_sedona": "#9467bd",  # Purple
    "clickhouse": "#8c564b"      # Brown
}

ENGINE_LABELS = {
    "sedonadb": "SedonaDB",
    "duckdb": "DuckDB",
    "spatial_polars": "Polaris (Spatial Polars)",
    "geopandas": "GeoPandas",
    "apache_sedona": "Apache Sedona",
    "clickhouse": "ClickHouse"
}


def load_results(results_dir: str, scale_factor: str, mode: str):
    """Load JSON result files matching the scale factor and mode."""
    pattern = os.path.join(results_dir, f"*sf{scale_factor}*.json")
    files = glob.glob(pattern)

    data = {}  # engine -> {query -> mean_time}
    for file_path in files:
        try:
            with open(file_path, "r") as f:
                content = json.load(f)

            if isinstance(content, dict) and "results" in content:
                for suite in content["results"]:
                    engine = suite.get("engine", "")
                    if not engine:
                        continue
                    if engine not in data:
                        data[engine] = {}

                    for q_item in suite.get("results", []):
                        q = q_item.get("query", "").lower()
                        t = q_item.get("time_seconds")
                        if q and t is not None and q_item.get("status") == "success":
                            data[engine][q] = float(t)
        except Exception as e:
            print(f"[WARN] Failed to parse {file_path}: {e}")

    return data


def generate_markdown_summary(data, output_file: str, sf: str, mode: str):
    """Generate markdown table with query timings and geometric means."""
    engines = sorted(data.keys())
    if not engines:
        print("[WARN] No engine results to summarize.")
        return

    lines = []
    lines.append(f"# SpatialBench Performance Summary (SF{sf}, Mode: {mode.capitalize()})\n")
    lines.append("| Query | " + " | ".join(ENGINE_LABELS.get(e, e) for e in engines) + " |")
    lines.append("|:---|" + "|".join([":---:"] * len(engines)) + "|")

    totals = {e: 0.0 for e in engines}
    valid_counts = {e: 0 for e in engines}
    log_sums = {e: 0.0 for e in engines}

    active_queries = [q for q in QUERY_NAMES if any(q in data[e] for e in engines)]

    for q in active_queries:
        row = [q.upper()]
        for e in engines:
            t = data[e].get(q)
            if t is not None:
                row.append(f"{t:.2f}s")
                totals[e] += t
                valid_counts[e] += 1
                log_sums[e] += np.log(max(t, 1e-6))
            else:
                row.append("N/A")
        lines.append("| " + " | ".join(row) + " |")

    lines.append("| **Total Time** | " + " | ".join(f"**{totals[e]:.2f}s**" for e in engines) + " |")

    # Geometric mean
    geom_means = []
    for e in engines:
        if valid_counts[e] > 0:
            gm = np.exp(log_sums[e] / valid_counts[e])
            geom_means.append(f"{gm:.2f}s")
        else:
            geom_means.append("N/A")
    lines.append("| **Geom. Mean** | " + " | ".join(geom_means) + " |")

    summary_content = "\n".join(lines) + "\n"
    with open(output_file, "w") as f:
        f.write(summary_content)
    print(f"[OK] Summary table written to: {output_file}")
    print("\n" + summary_content)


def plot_execution_times(data, output_pdf: str, output_png: str, sf: str, mode: str):
    """Generate publication-ready execution time bar chart."""
    if not HAS_MATPLOTLIB:
        print("[WARN] Matplotlib not found, skipping figure generation.")
        return

    engines = sorted(data.keys())
    if not engines:
        return

    active_queries = [q for q in QUERY_NAMES if any(q in data[e] for e in engines)]
    if not active_queries:
        return

    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, ax = plt.subplots(figsize=(max(6, len(active_queries) * 1.5), 4.5))

    n_queries = len(active_queries)
    n_engines = len(engines)
    width = 0.8 / n_engines
    x = np.arange(n_queries)

    for i, engine in enumerate(engines):
        times = [data[engine].get(q, 0.001) for q in active_queries]
        label = ENGINE_LABELS.get(engine, engine)
        color = ENGINE_COLORS.get(engine, None)
        offset = (i - (n_engines - 1) / 2) * width
        rects = ax.bar(x + offset, times, width, label=label, color=color, edgecolor="black", linewidth=0.5)

    ax.set_ylabel("Execution Time (seconds, log scale)", fontsize=11, fontweight="bold")
    ax.set_title(f"Sedona SpatialBench Query Runtimes (SF{sf}, {mode.capitalize()} Mode)", fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in active_queries], fontsize=10, fontweight="bold")
    ax.set_yscale("log")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig(output_pdf, bbox_inches="tight")
    plt.savefig(output_png, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"[OK] Execution time figure saved: {output_pdf} and {output_png}")


def plot_category_breakdown(data, output_pdf: str, output_png: str, sf: str):
    """Plot total execution time aggregated across query categories."""
    if not HAS_MATPLOTLIB:
        return

    engines = sorted(data.keys())
    categories = [cat for cat, q_list in QUERY_CATEGORIES.items() if any(q in data[e] for e in engines for q in q_list)]
    if not categories:
        return

    cat_times = {e: [] for e in engines}
    for cat in categories:
        q_list = QUERY_CATEGORIES[cat]
        for e in engines:
            total_cat = sum(data[e].get(q, 0.0) for q in q_list if q in data[e])
            cat_times[e].append(total_cat)

    fig, ax = plt.subplots(figsize=(max(6, len(categories) * 2), 4.5))
    x = np.arange(len(categories))
    width = 0.8 / len(engines)

    for i, e in enumerate(engines):
        offset = (i - (len(engines) - 1) / 2) * width
        ax.bar(x + offset, cat_times[e], width, label=ENGINE_LABELS.get(e, e), color=ENGINE_COLORS.get(e), edgecolor="black", linewidth=0.5)

    ax.set_ylabel("Total Time (seconds)", fontsize=11, fontweight="bold")
    ax.set_title(f"Query Category Runtime Breakdown (SF{sf})", fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=9, fontweight="bold")
    ax.legend(frameon=True, fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig(output_pdf, bbox_inches="tight")
    plt.savefig(output_png, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"[OK] Category breakdown figure saved: {output_pdf} and {output_png}")


def main():
    parser = argparse.ArgumentParser(description="Summarize and plot SpatialBench results")
    parser.add_argument("--results-dir", type=str, required=True, help="Directory containing JSON results")
    parser.add_argument("--output-dir", type=str, required=True, help="Directory to save plots and tables")
    parser.add_argument("--sf", type=str, default="10", help="Scale factor (e.g., 1 or 10)")
    parser.add_argument("--mode", type=str, default="default", help="Mode: default or optimized")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    data = load_results(args.results_dir, args.sf, args.mode)

    if not data:
        print(f"[INFO] No results found matching SF{args.sf} and mode '{args.mode}' in {args.results_dir}")
        return

    summary_path = os.path.join(args.output_dir, f"summary_sf{args.sf}_{args.mode}.md")
    generate_markdown_summary(data, summary_path, args.sf, args.mode)

    pdf_bar = os.path.join(args.output_dir, f"runtimes_sf{args.sf}_{args.mode}.pdf")
    png_bar = os.path.join(args.output_dir, f"runtimes_sf{args.sf}_{args.mode}.png")
    plot_execution_times(data, pdf_bar, png_bar, args.sf, args.mode)

    pdf_cat = os.path.join(args.output_dir, f"categories_sf{args.sf}.pdf")
    png_cat = os.path.join(args.output_dir, f"categories_sf{args.sf}.png")
    plot_category_breakdown(data, pdf_cat, png_cat, args.sf)


if __name__ == "__main__":
    main()
