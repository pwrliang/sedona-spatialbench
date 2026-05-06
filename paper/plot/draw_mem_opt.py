#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.

import argparse
import json
import sys
import os
import re
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# --- Configuration for Research Quality Figures ---
sns.set_theme(style="whitegrid", context="paper", font_scale=1.5)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['savefig.bbox'] = 'tight'

# Define a list of hatch patterns for high readability
HATCH_PATTERNS = ['/', '\\', '.', 'o', '*', 'x', '+', '-', '//']

no_opt_label = "Using cudaMalloc"
opt_label = "Memory Alloc. with RMM"


def load_data_to_df(results_dir: Path, query_filter: list[str] = None) -> pd.DataFrame:
    """Load JSON files into a Pandas DataFrame."""
    data_points = []

    if not results_dir.exists():
        print(f"Error: Directory {results_dir} does not exist.")
        return pd.DataFrame()

    files = list(results_dir.glob("sedonadb_gpu_results.json"))
    if not files:
        print(f"Warning: No *_results.json files found in {results_dir}")
        return pd.DataFrame()

    for json_file in files:
        with open(json_file) as f:
            try:
                data = json.load(f)
                for suite in data.get("results", []):
                    # Determine optimization status from directory name
                    opt_status = no_opt_label if "noopt" in str(results_dir) else opt_label

                    for r in suite.get("results", []):
                        query_id = r["query"]
                        if query_filter and query_id not in query_filter:
                            continue

                        status = r.get("status", "unknown")
                        time_val = r.get("time_seconds")

                        display_time = time_val if status == "success" else 0

                        data_points.append({
                            "Configuration": opt_status,
                            "Query": query_id.upper(),
                            "Time (s)": display_time,
                            "Status": status
                        })
            except json.JSONDecodeError:
                print(f"Warning: Could not decode {json_file}")

    df = pd.DataFrame(data_points)
    if df.empty:
        return df

    try:
        df['sort_key'] = df['Query'].str.extract(r'(\d+)').astype(float)
        df = df.sort_values(by=['sort_key', 'Configuration']).drop(columns=['sort_key'])
    except Exception:
        df = df.sort_values(by=['Query', 'Configuration'])
    return df


def print_statistics(df: pd.DataFrame, sf_label: str):
    """Print raw times and calculated speedups to the console."""
    if df.empty:
        return

    # Filter for successful runs to calculate accurate speedup
    success_df = df[df['Status'] == 'success']

    if success_df.empty:
        print(f"\n--- No successful runs to report for SF={sf_label} ---")
        return

    try:
        # Pivot the table to get 'No Opt' and 'Opt' columns side by side per Query
        pivot_df = success_df.pivot(index='Query', columns='Configuration', values='Time (s)')

        # Ensure both columns exist before calculating speedup
        if no_opt_label in pivot_df.columns and opt_label in pivot_df.columns:
            # Speedup = Time without optimization / Time with optimization
            pivot_df['Speedup (x)'] = pivot_df[no_opt_label] / pivot_df[opt_label]

            # Sort the queries numerically for cleaner output
            pivot_df = pivot_df.reset_index()
            pivot_df['sort_key'] = pivot_df['Query'].str.extract(r'(\d+)').astype(float)
            pivot_df = pivot_df.sort_values('sort_key').drop(columns=['sort_key']).set_index('Query')

            print(f"\n=== Scale Factor: {sf_label} ===")
            # Format the output to 2 decimal places
            print(pivot_df[[no_opt_label, opt_label, 'Speedup (x)']].apply(lambda x: round(x, 2)).to_string())
        else:
            print(f"\n=== Scale Factor: {sf_label} (Incomplete Data) ===")
            print(pivot_df.to_string())

    except Exception as e:
        print(f"\nCould not calculate statistics for SF={sf_label}: {e}")


def draw_subplot1(ax, df, log_scale, title):
    """Helper function to draw bars on a specific axes."""
    if df.empty:
        ax.text(0.5, 0.5, "No Data", ha='center', va='center')
        ax.set_xlabel(title, fontweight='bold', labelpad=15)
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
    custom_palette = [full_set2[1], full_set2[0]]  # Salmon/Skyblue approximation

    # Draw Barplot
    sns.barplot(
        data=plot_df,
        x="Query",
        y="Time (s)",
        hue="Configuration",
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
        ax.set_ylim(bottom=min_success_time * 0.5)

    ax.set_ylabel("Execution Time (s)", fontweight='bold')

    # Place title at the bottom by treating it as the x-axis label
    ax.set_xlabel(title, fontweight='bold', labelpad=15, fontsize=16)
    ax.set_title("")

    # Legend
    ax.legend(loc='upper left', frameon=False, fontsize=12, bbox_to_anchor=(0, 0.99))

    # Grid
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)

    # Annotations
    configs_ordered = [t.get_text() for t in ax.get_legend().get_texts()]
    y_offset = max_success_time * 0.02

    for container_idx, container in enumerate(ax.containers):
        if container_idx >= len(configs_ordered): break
        config_name = configs_ordered[container_idx]

        for bar in container:
            x = bar.get_x() + bar.get_width() / 2
            if pd.isna(x): continue

            query_idx = int(round(x))
            xticklabels = ax.get_xticklabels()

            if 0 <= query_idx < len(xticklabels):
                query_name = xticklabels[query_idx].get_text()
                res = plot_df[(plot_df['Configuration'] == config_name) & (plot_df['Query'] == query_name)]
                if res.empty: continue

                status = res.iloc[0]['Status']

                # Only annotate if it's an error or timeout
                if status != "success":
                    label = status.upper()
                    label_y = y_offset if not log_scale else min_success_time
                    ax.text(x, label_y, label, ha='center', va='bottom',
                            fontsize=12, rotation=90, color='red', fontweight='bold')


def draw_subplot2(ax, spill_dir: Path, title: str):
    """Parses memory budget logs and draws a line plot for Q11 performance."""
    data = []

    if not spill_dir.exists():
        print(f"Warning: Spill directory {spill_dir} does not exist.")
        ax.text(0.5, 0.5, "No Spill Directory Found", ha='center', va='center')
        ax.set_xlabel(title, fontweight='bold', labelpad=15, fontsize=16)
        return

    pattern = re.compile(r"results_SF_\d+_MEM_LIMIT_(\d+)gb")

    for dirname in spill_dir.iterdir():
        if not dirname.is_dir():
            continue

        match = pattern.search(dirname.name)
        if match:
            mem_limit = int(match.group(1))
            json_file = dirname / "sedonadb_gpu_results.json"
            if json_file.exists():
                with open(json_file, 'r') as f:
                    try:
                        content = json.load(f)
                        # Assumes the structure matches script #2: results[0].total_time
                        results_list = content.get("results", [])
                        if results_list:
                            total_time = results_list[0].get("total_time")
                            if total_time is not None:
                                data.append((mem_limit, total_time))
                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode {json_file}")

    data.sort(key=lambda x: x[0])

    if not data:
        ax.text(0.5, 0.5, "No Memory Spill Data", ha='center', va='center')
        ax.set_xlabel(title, fontweight='bold', labelpad=15, fontsize=16)
        return

    mem_limits = [x[0] for x in data]
    times = [x[1] for x in data]

    # Draw Lineplot
    ax.plot(mem_limits, times, marker='o', linestyle='-', linewidth=2.5,
            markersize=10, color=sns.color_palette("Set2")[2], label="Q11")

    # Styling Spines (matching subplot 1)
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)
        spine.set_visible(True)

    # Grid and Legends
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=12)

    # Labels and Titles (Title placed at bottom to match ax1)
    ax.set_ylabel('Running Time (s)', fontweight='bold')
    ax.set_xlabel(title, fontweight='bold', labelpad=15, fontsize=16)
    ax.set_title("")


def plot_benchmark(df_sf10: pd.DataFrame, root_dir: Path, output_file: str = None, log_scale: bool = False):
    """Draw two subplots side by side."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4.5))

    # Draw Subplots (Titles will be rendered at the bottom via x-labels)
    draw_subplot1(ax1, df_sf10, log_scale, "(a) Scale Factor: 10")

    # Point Subplot 2 specifically to the "spill" subdirectory
    spill_dir = root_dir / "spill"
    draw_subplot2(ax2, spill_dir, "(b) Memory Budget (GB)")

    plt.tight_layout()

    if output_file:
        print(f"\nSaving figure to {output_file}...")
        plt.savefig(output_file, format='pdf', dpi=300)
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-dir", type=str, required=True,
                        help="Base directory containing 'results_*' subfolders and the 'spill' folder")
    parser.add_argument("--output", type=str, default="mem_comp.pdf")
    parser.add_argument("--log-scale", action="store_true")
    parser.add_argument("--queries", type=str)
    args = parser.parse_args()

    q_filter = [q.strip().lower() for q in args.queries.split(',')] if args.queries else None
    base_path = Path(args.root_dir)

    # Combine SF10 data (Subplot 1 expects these directly in root_dir)
    df_sf10_noopt = load_data_to_df(base_path / "results_mem_noopt_SF_10", q_filter)
    df_sf10_opt = load_data_to_df(base_path / "results_SF_10", q_filter)
    df_sf10 = pd.concat([df_sf10_noopt, df_sf10_opt]).reset_index(drop=True)

    if df_sf10.empty:
        print("Error: No data found in the specified directories for Subplot A.")
        sys.exit(1)

    # Print raw numbers and speedups to console
    print_statistics(df_sf10, "10")

    # Resolve output path to be in the same directory as this script
    script_dir = Path(__file__).resolve().parent
    output_path = script_dir / args.output

    # Pass the base_path to plot_benchmark, which will handle routing to the spill folder
    plot_benchmark(df_sf10, base_path, str(output_path), args.log_scale)


if __name__ == "__main__":
    main()
