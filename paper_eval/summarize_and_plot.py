#!/usr/bin/env python3
"""
Summarize SpatialBench results and generate publication-quality figures
for Section 5 of the VLDB 2027 paper.
Categorizes query runtimes across the 4 functional categories defined in Section 4.1.
Marks failed and timed-out queries explicitly in place of missing bars.
Supports PostGIS evaluated under both indexed and no-index configurations.
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

# Four functional categories defined in Section 4.1 of the paper
QUERY_CATEGORIES = [
    ("Filtering & Range Scans", ["q1", "q2", "q3"]),
    ("Selective Join Planning", ["q4", "q6"]),
    ("Geometric Computation", ["q5", "q7", "q9"]),
    ("Heavy Spatial Joins", ["q8", "q10", "q11", "q12"])
]

ENGINE_ORDER = [
    "postgis",
    "postgis_noindex",
    "duckdb",
    "sedonadb",
    "clickhouse",
    "spatial_polars",
    "geopandas",
    "apache_sedona",
    "trino"
]

ENGINE_COLORS = {
    "postgis": "#336791",        # PostGIS Slate Blue
    "postgis_noindex": "#85b6d9",# PostGIS Light Steel Blue
    "duckdb": "#ff7f0e",         # Orange
    "sedonadb": "#1f77b4",       # SedonaDB Royal Blue
    "clickhouse": "#8c564b",     # Brown
    "spatial_polars": "#2ca02c", # Polaris Green
    "geopandas": "#d62728",      # Red
    "apache_sedona": "#9467bd",  # Purple
    "trino": "#17becf"           # Teal
}


ENGINE_HATCHES = {
    "postgis": "//",
    "postgis_noindex": "\\",
    "duckdb": "xx",
    "sedonadb": "..",
    "clickhouse": "++",
    "spatial_polars": "**",
    "geopandas": "--",
    "apache_sedona": "oo",
    "trino": "||"
}

ENGINE_LABELS = {
    "postgis": "PostGIS (Indexed)",
    "postgis_noindex": "PostGIS (No Index)",
    "duckdb": "DuckDB",
    "sedonadb": "SedonaDB",
    "clickhouse": "ClickHouse",
    "spatial_polars": "Polaris (Spatial Polars)",
    "geopandas": "GeoPandas",
    "apache_sedona": "Apache Sedona",
    "trino": "Trino"
}


def load_results(results_dir: str, scale_factor: str, mode: str):
    """Load JSON result files matching the scale factor and mode."""
    pattern = os.path.join(results_dir, f"*sf{scale_factor}*.json")
    files = glob.glob(pattern)

    data = {}  # engine -> {query -> mean_time}
    for file_path in files:
        fname = os.path.basename(file_path).lower()
        if mode and mode.lower() not in fname:
            continue

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
                        status = q_item.get("status", "")
                        if q:
                            if status == "success" and t is not None:
                                data[engine][q] = float(t)
                            elif status in ("failed", "timeout", "error"):
                                data[engine][q] = "FAILED"
        except Exception as e:
            print(f"[WARN] Failed to parse {file_path}: {e}")

    return data


def generate_markdown_summary(data, output_file: str, sf: str, mode: str):
    """Generate markdown table with query timings and geometric means."""
    engines = [e for e in ENGINE_ORDER if e in data]
    if not engines:
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
            val = data[e].get(q)
            if isinstance(val, (int, float)):
                row.append(f"{val:.2f}s")
                totals[e] += val
                valid_counts[e] += 1
                log_sums[e] += np.log(max(val, 1e-6))
            elif val == "FAILED":
                row.append("FAILED")
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


def plot_categorized_execution_times(data, output_pdf: str, output_png: str, sf: str, mode: str):
    """
    Generate publication-quality 4-panel figure categorized according to Section 4.1:
      (a) Filtering & Range Scans (Q1-Q3)
      (b) Selective Join Planning (Q4, Q6)
      (c) Geometric Computation (Q5, Q7, Q9)
      (d) Heavy Spatial Joins (Q8, Q10-Q12)
    Uses subgridspec to preserve equal bar widths across panels with varying query counts.
    Explicitly marks failed and timed out queries with a vertical 'FAIL' label in engine color.
    """
    if not HAS_MATPLOTLIB:
        print("[WARN] Matplotlib not found, skipping figure generation.")
        return

    engines = [e for e in ENGINE_ORDER if e in data]
    if not engines:
        engines = sorted(data.keys())
    if not engines:
        return

    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    plt.rcParams["hatch.linewidth"] = 0.55

    fig = plt.figure(figsize=(7.2, 4.2))
    gs = fig.add_gridspec(2, 1, height_ratios=[1, 1], hspace=0.38)

    # Top row: 3 queries vs 2 queries
    w_top = [len(QUERY_CATEGORIES[0][1]), len(QUERY_CATEGORIES[1][1])]
    # Bottom row: 3 queries vs 4 queries
    w_bot = [len(QUERY_CATEGORIES[2][1]), len(QUERY_CATEGORIES[3][1])]

    gs_top = gs[0].subgridspec(1, 2, width_ratios=w_top, wspace=0.22)
    gs_bot = gs[1].subgridspec(1, 2, width_ratios=w_bot, wspace=0.22)

    axes = [
        fig.add_subplot(gs_top[0]),
        fig.add_subplot(gs_top[1]),
        fig.add_subplot(gs_bot[0]),
        fig.add_subplot(gs_bot[1])
    ]

    n_eng = len(engines)
    bar_width = 0.82 / n_eng

    for ax_idx, (cat_name, queries) in enumerate(QUERY_CATEGORIES):
        ax = axes[ax_idx]
        x = np.arange(len(queries))

        for i, eng in enumerate(engines):
            vals = [
                data[eng].get(q) if isinstance(data[eng].get(q), (int, float)) else np.nan
                for q in queries
            ]
            offset = (i - (n_eng - 1) / 2) * bar_width
            eng_color = ENGINE_COLORS.get(eng, "#555555")

            ax.bar(
                x + offset,
                vals,
                bar_width,
                label=ENGINE_LABELS.get(eng, eng),
                color=eng_color,
                edgecolor="black",
                linewidth=0.35,
                hatch=ENGINE_HATCHES.get(eng, "")
            )

            # Explicitly mark failed and timed out queries in place of missing bars
            for q_idx, q in enumerate(queries):
                val = data[eng].get(q)
                if not isinstance(val, (int, float)):
                    pos_x = x[q_idx] + offset
                    ax.plot([pos_x, pos_x], [0.005, 0.007], color=eng_color, linewidth=1.3)
                    ax.text(
                        pos_x,
                        0.008,
                        "FAIL",
                        rotation=90,
                        ha="center",
                        va="bottom",
                        fontsize=4.6,
                        fontweight="bold",
                        color=eng_color
                    )

        ax.set_xticks(x)
        ax.set_xticklabels([q.upper() for q in queries], fontsize=8.5, fontweight="bold")
        ax.set_title(f"({chr(97+ax_idx)}) {cat_name}", fontsize=9, fontweight="bold", pad=4)
        ax.set_yscale("log")
        ax.set_ylim(0.005, 1200)
        ax.grid(axis="y", linestyle="--", alpha=0.5, linewidth=0.5)

        if ax_idx in (0, 2):
            ax.set_ylabel("Execution Time (s, log)", fontsize=8, fontweight="bold")

    # Shared legend placed cleanly at top without overlapping titles
    handles, labels = axes[0].get_legend_handles_labels()
    ncol = 4 if n_eng > 4 else n_eng
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.99),
        ncol=ncol,
        fontsize=7.5,
        frameon=True,
        edgecolor="#cccccc"
    )

    fig.subplots_adjust(top=0.86, bottom=0.07, left=0.09, right=0.98)
    plt.savefig(output_pdf, bbox_inches="tight")
    plt.savefig(output_png, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"[OK] Categorized execution time figure saved: {output_pdf} and {output_png}")

    # Generate individual category figures as well
    out_dir = os.path.dirname(output_pdf)
    base_name = os.path.splitext(os.path.basename(output_pdf))[0]
    for ax_idx, (cat_name, queries) in enumerate(QUERY_CATEGORIES):
        fig_cat, ax_cat = plt.subplots(figsize=(max(3.2, len(queries) * 1.2), 3.2))
        x = np.arange(len(queries))
        for i, eng in enumerate(engines):
            vals = [
                data[eng].get(q) if isinstance(data[eng].get(q), (int, float)) else np.nan
                for q in queries
            ]
            offset = (i - (n_eng - 1) / 2) * bar_width
            eng_color = ENGINE_COLORS.get(eng, "#555555")

            ax_cat.bar(
                x + offset,
                vals,
                bar_width,
                label=ENGINE_LABELS.get(eng, eng),
                color=eng_color,
                edgecolor="black",
                linewidth=0.35,
                hatch=ENGINE_HATCHES.get(eng, "")
            )

            # Explicitly mark failed queries in individual category figures
            for q_idx, q in enumerate(queries):
                val = data[eng].get(q)
                if not isinstance(val, (int, float)):
                    pos_x = x[q_idx] + offset
                    ax_cat.plot([pos_x, pos_x], [0.005, 0.007], color=eng_color, linewidth=1.3)
                    ax_cat.text(
                        pos_x,
                        0.008,
                        "FAIL",
                        rotation=90,
                        ha="center",
                        va="bottom",
                        fontsize=5.5,
                        fontweight="bold",
                        color=eng_color
                    )

        ax_cat.set_xticks(x)
        ax_cat.set_xticklabels([q.upper() for q in queries], fontsize=9, fontweight="bold")
        ax_cat.set_title(f"{cat_name}", fontsize=10, fontweight="bold")
        ax_cat.set_yscale("log")
        ax_cat.set_ylim(0.005, 1200)
        ax_cat.set_ylabel("Execution Time (s, log)", fontsize=9, fontweight="bold")
        ax_cat.grid(axis="y", linestyle="--", alpha=0.5, linewidth=0.5)
        ax_cat.legend(frameon=True, fontsize=7.0, loc="upper right", ncol=2)
        plt.tight_layout()
        cat_pdf = os.path.join(out_dir, f"{base_name}_cat{ax_idx+1}.pdf")
        cat_png = os.path.join(out_dir, f"{base_name}_cat{ax_idx+1}.png")
        plt.savefig(cat_pdf, bbox_inches="tight")
        plt.savefig(cat_png, dpi=300, bbox_inches="tight")
        plt.close()


def plot_category_breakdown(data, output_pdf: str, output_png: str, sf: str):
    """Plot total execution time aggregated across query categories."""
    if not HAS_MATPLOTLIB:
        return

    engines = [e for e in ENGINE_ORDER if e in data]
    if not engines:
        engines = sorted(data.keys())
    if not engines:
        return

    categories = [cat_name for cat_name, q_list in QUERY_CATEGORIES]
    cat_times = {e: [] for e in engines}

    for cat_name, q_list in QUERY_CATEGORIES:
        for e in engines:
            total_cat = sum(data[e].get(q) for q in q_list if isinstance(data[e].get(q), (int, float)))
            cat_times[e].append(total_cat)

    fig, ax = plt.subplots(figsize=(max(6, len(categories) * 1.8), 4.0))
    x = np.arange(len(categories))
    width = 0.80 / len(engines)

    for i, e in enumerate(engines):
        offset = (i - (len(engines) - 1) / 2) * width
        ax.bar(
            x + offset,
            cat_times[e],
            width,
            label=ENGINE_LABELS.get(e, e),
            color=ENGINE_COLORS.get(e, "#555555"),
            edgecolor="black",
            linewidth=0.4,
            hatch=ENGINE_HATCHES.get(e, "")
        )

    ax.set_ylabel("Total Time (seconds)", fontsize=10, fontweight="bold")
    # ax.set_title (handled by LaTeX caption)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=8.5, fontweight="bold")
    ncol = 4 if len(engines) > 4 else len(engines)
    ax.legend(frameon=True, fontsize=7.8, ncol=ncol, loc="upper center", bbox_to_anchor=(0.5, 0.98))
    ax.grid(axis="y", linestyle="--", alpha=0.6)

    # Provide headroom for legend
    all_vals = [val for vals in cat_times.values() for val in vals]
    if all_vals:
        ax.set_ylim(0, max(all_vals) * 1.25)

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
    plot_categorized_execution_times(data, pdf_bar, png_bar, args.sf, args.mode)

    pdf_cat = os.path.join(args.output_dir, f"categories_sf{args.sf}.pdf")
    png_cat = os.path.join(args.output_dir, f"categories_sf{args.sf}.png")
    plot_category_breakdown(data, pdf_cat, png_cat, args.sf)


if __name__ == "__main__":
    main()
