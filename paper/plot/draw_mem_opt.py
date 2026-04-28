#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.

import argparse
import json
import sys
import os
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

    files = list(results_dir.glob("*_results.json"))
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


def draw_subplot(ax, df, log_scale, title):
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

    # Remove the top title if seaborn created one automatically
    ax.set_title("")

    # Legend
    ax.legend(loc='upper left', frameon=False, fontsize=12, bbox_to_anchor=(0, 0.99))

    # Grid
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)

    # Annotations (Only keeping Timeout/Error warnings, removed success numbers)
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


def plot_benchmark(df_sf1: pd.DataFrame, df_sf10: pd.DataFrame, output_file: str = None, log_scale: bool = False):
    """Draw two subplots (SF1 and SF10) side by side."""

    # Width calculation: Single plot width * 2 roughly
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4.5))

    # Draw Subplots (Titles will be rendered at the bottom via x-labels)
    draw_subplot(ax1, df_sf1, log_scale, "(a) Scale Factor: 1")
    draw_subplot(ax2, df_sf10, log_scale, "(b) Scale Factor: 10")

    plt.tight_layout()

    if output_file:
        print(f"\nSaving figure to {output_file}...")
        plt.savefig(output_file, format='pdf', dpi=300)
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-dir", type=str, required=True,
                        help="Base directory containing 'results_*' subfolders")
    parser.add_argument("--output", type=str, default="mem_comp.pdf")
    parser.add_argument("--log-scale", action="store_true")
    parser.add_argument("--queries", type=str)
    args = parser.parse_args()

    q_filter = [q.strip().lower() for q in args.queries.split(',')] if args.queries else None
    base_path = Path(args.root_dir)

    # Combine SF1 data
    df_sf1_noopt = load_data_to_df(base_path / "results_mem_noopt_SF_1", q_filter)
    df_sf1_opt = load_data_to_df(base_path / "results_SF_1", q_filter)
    df_sf1 = pd.concat([df_sf1_noopt, df_sf1_opt]).reset_index(drop=True)

    # Combine SF10 data
    df_sf10_noopt = load_data_to_df(base_path / "results_mem_noopt_SF_10", q_filter)
    df_sf10_opt = load_data_to_df(base_path / "results_SF_10", q_filter)
    df_sf10 = pd.concat([df_sf10_noopt, df_sf10_opt]).reset_index(drop=True)

    if df_sf1.empty and df_sf10.empty:
        print("Error: No data found in the specified directories.")
        sys.exit(1)

    # Print raw numbers and speedups to console
    print_statistics(df_sf1, "1")
    print_statistics(df_sf10, "10")

    # Resolve output path to be in the same directory as this script
    script_dir = Path(__file__).resolve().parent
    output_path = script_dir / args.output

    plot_benchmark(df_sf1, df_sf10, str(output_path), args.log_scale)


if __name__ == "__main__":
    main()
