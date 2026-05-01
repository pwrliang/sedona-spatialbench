#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.

import argparse
import json
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# --- Configuration for Research Quality Figures ---
sns.set_theme(style="whitegrid", context="paper", font_scale=2.5)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['savefig.bbox'] = 'tight'

# Define a list of hatch patterns for high readability
HATCH_PATTERNS = ['/', '\\', '.', 'o', '*', 'x', '+', '-', '//']

# --- Define the Engine Order Globally ---
ENGINE_ORDER = ["PostGIS", "GeoPandas", "DuckDB", "SedonaDB"]

def load_data_to_df(results_dir: Path, query_filter: list[str] = None) -> pd.DataFrame:
    """Load JSON files into a Pandas DataFrame, handling Errors and Timeouts."""
    data_points = []

    if not results_dir.exists():
        print(f"Error: Directory {results_dir} does not exist.")
        sys.exit(1)

    files = list(results_dir.glob("*_results.json"))
    if not files:
        print(f"Warning: No *_results.json files found in {results_dir}")
        return pd.DataFrame()

    for json_file in files:
        with open(json_file) as f:
            try:
                data = json.load(f)
                for suite in data.get("results", []):
                    engine = suite.get("engine", "unknown")
                    for r in suite.get("results", []):
                        query_id = r["query"]
                        if query_filter and query_id not in query_filter:
                            continue

                        status = r.get("status", "unknown")
                        time_val = r.get("time_seconds")

                        # Success gets actual time, Timeout/Error gets 0 for no bar
                        display_time = time_val if status == "success" else 0

                        data_points.append({
                            "Engine": engine,
                            "Query": query_id.upper(),
                            "Time (s)": display_time,
                            "Status": status
                        })
            except json.JSONDecodeError:
                print(f"Warning: Could not decode {json_file}")

    df = pd.DataFrame(data_points)
    if df.empty:
        return df

    # --- Convert Engine to Categorical to enforce sorting order ---
    df['Engine'] = pd.Categorical(df['Engine'], categories=ENGINE_ORDER, ordered=True)

    try:
        df['sort_key'] = df['Query'].str.extract(r'(\d+)').astype(float)
        df = df.sort_values(by=['sort_key', 'Engine']).drop(columns=['sort_key'])
    except Exception:
        df = df.sort_values(by=['Query', 'Engine'])
    return df

def draw_subplot(ax, df, log_scale, title):
    """Helper function to draw bars on a specific axes."""
    if df.empty:
        ax.text(0.5, 0.5, "No Data", ha='center', va='center')
        ax.set_title(title)
        return

    # Calculate scale limits for this subplot
    max_success_time = df[df['Status'] == 'success']['Time (s)'].max()
    if pd.isna(max_success_time): max_success_time = 10
    min_success_time = df[df['Status'] == 'success']['Time (s)'].min()
    if pd.isna(min_success_time): min_success_time = 0.1

    # Handle Log Scale Epsilon
    plot_df = df.copy()
    if log_scale:
        epsilon = min_success_time * 0.001
        plot_df.loc[plot_df['Status'].isin(['error', 'timeout']), 'Time (s)'] = epsilon

    # Custom Palette
    full_set2 = sns.color_palette("Set2")
    custom_palette = [full_set2[2], full_set2[0], full_set2[5], full_set2[1]]

    # Draw Barplot
    sns.barplot(
        data=plot_df,
        x="Query",
        y="Time (s)",
        hue="Engine",
        hue_order=ENGINE_ORDER, # --- Force Seaborn to use this exact order ---
        edgecolor="black",
        linewidth=1.0,
        errorbar=None,
        width=0.6,
        palette=custom_palette,
        ax=ax
    )

    # Styling Spines
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)
        spine.set_visible(True)

    # Apply Hatches
    for i, container in enumerate(ax.containers):
        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        for bar in container:
            bar.set_hatch(hatch)

    # Axis Formatting
    if log_scale:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
        # Multiply top by a factor of 5 to 10 for log scale padding
        ax.set_ylim(bottom=min_success_time * 0.5, top=max_success_time * 15)
    else:
        # Multiply top by 1.2 to add 20% padding for linear scale
        ax.set_ylim(bottom=0, top=max_success_time * 1.2)

    ax.set_ylabel("Execution Time (s)", fontweight='bold', fontsize=16)
    ax.set_xlabel(title, fontweight='bold', fontsize=16)
    ax.tick_params(axis='both', which='major', labelsize=15)
    # Legend
    ax.legend(loc='upper left', ncol=2, frameon=False, fontsize=15)

    # Grid
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)

    # Annotations
    engines_ordered = [t.get_text() for t in ax.get_legend().get_texts()]
    y_offset = max_success_time * 0.02

    for container_idx, container in enumerate(ax.containers):
        if container_idx >= len(engines_ordered): break
        engine_name = engines_ordered[container_idx]

        for bar in container:
            x = bar.get_x() + bar.get_width() / 2
            if pd.isna(x): continue

            # Find the query corresponding to this bar
            # We must be careful with x-coordinates mapping to integer indices
            query_idx = int(round(x))
            xticklabels = ax.get_xticklabels()

            if 0 <= query_idx < len(xticklabels):
                query_name = xticklabels[query_idx].get_text()
                res = plot_df[(plot_df['Engine'] == engine_name) & (plot_df['Query'] == query_name)]
                if res.empty: continue

                status = res.iloc[0]['Status']
                time_val = df[(df['Engine'] == engine_name) & (df['Query'] == query_name)].iloc[0]['Time (s)']

                if status == "success":
                    if not log_scale:
                        ax.text(x, bar.get_height() + y_offset, f'{time_val:.2f}s',
                                ha='center', va='bottom', fontsize=8, rotation=90)
                else:
                    label = status.upper()
                    # For log scale, place error labels at bottom; for linear, slightly above 0
                    label_y = y_offset if not log_scale else min_success_time
                    ax.text(x, label_y, label, ha='center', va='bottom',
                            fontsize=10, rotation=90, color='red', fontweight='bold')

def plot_benchmark(df_sf1: pd.DataFrame, df_sf10: pd.DataFrame, output_file: str = None, log_scale: bool = False):
    """Draw two subplots (SF1 and SF10) side by side."""

    # Determine figure size based on the dataset with more queries
    num_queries = 0
    if not df_sf1.empty:
        num_queries = max(num_queries, df_sf1['Query'].nunique())
    if not df_sf10.empty:
        num_queries = max(num_queries, df_sf10['Query'].nunique())

    # Width calculation: Single plot width * 2 roughly
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 5))

    # Draw Subplots
    draw_subplot(ax1, df_sf1, log_scale, "(a) Scale Factor: 1")
    draw_subplot(ax2, df_sf10, log_scale, "(b) Scale Factor: 10")

    plt.tight_layout(w_pad=0.2)

    if output_file:
        print(f"Saving to {output_file}...")
        plt.savefig(output_file, dpi=300)
    else:
        plt.show()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=str, required=True,
                        help="Base directory containing 'sf1' and 'sf10' subfolders")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--log-scale", action="store_true")
    parser.add_argument("--queries", type=str)
    args = parser.parse_args()

    q_filter = [q.strip().lower() for q in args.queries.split(',')] if args.queries else None

    base_path = Path(args.results_dir)
    sf1_path = base_path / "results_SF_1"
    sf10_path = base_path / "results_SF_10"

    print(f"Loading SF1 results from: {sf1_path}")
    df_sf1 = load_data_to_df(sf1_path, q_filter)

    print(f"Loading SF10 results from: {sf10_path}")
    df_sf10 = load_data_to_df(sf10_path, q_filter)

    if df_sf1.empty and df_sf10.empty:
        print("Error: No data found in either sf1 or sf10 directories.")
        sys.exit(1)

    plot_benchmark(df_sf1, df_sf10, args.output, args.log_scale)

# --results-dir=/Users/liang/PycharmProjects/sedona-spatialbench/paper/logs/m7i.2xlarge --log-scale --output fig2.pdf
if __name__ == "__main__":
    main()
