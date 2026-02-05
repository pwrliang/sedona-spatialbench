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
sns.set_theme(style="whitegrid", context="paper", font_scale=1.8)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['savefig.bbox'] = 'tight'

# Define a list of hatch patterns for high readability
HATCH_PATTERNS = ['/', '\\', '.', 'o', '*', 'x', '+', '-', '//']

def load_data_to_df(results_dir: str, query_filter: list[str] = None) -> pd.DataFrame:
    """Load JSON files into a Pandas DataFrame, handling Errors and Timeouts."""
    data_points = []
    results_path = Path(results_dir)
    files = list(results_path.glob("*_results.json"))

    if not files:
        print(f"Error: No *_results.json files found in {results_dir}")
        sys.exit(1)

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
    try:
        df['sort_key'] = df['Query'].str.extract(r'(\d+)').astype(float)
        df = df.sort_values(by=['sort_key', 'Engine']).drop(columns=['sort_key'])
    except Exception:
        df = df.sort_values(by=['Query', 'Engine'])
    return df

def plot_benchmark(df: pd.DataFrame, output_file: str = None, log_scale: bool = False):
    """Draw a bar chart with hatch patterns for maximum readability."""

    max_success_time = df[df['Status'] == 'success']['Time (s)'].max()
    if pd.isna(max_success_time): max_success_time = 10
    min_success_time = df[df['Status'] == 'success']['Time (s)'].min()
    if pd.isna(min_success_time): min_success_time = 0.1

    if log_scale:
        epsilon = min_success_time * 0.001
        df.loc[df['Status'].isin(['error', 'timeout']), 'Time (s)'] = epsilon

    num_queries = df['Query'].nunique()
    plt.figure(figsize=(max(4.5, num_queries * 1.25), 6))
    full_set2 = sns.color_palette("Set2")

    # 2. Select 1st (0), 3rd (2), and 6th (5) colors
    # Note: If you have >3 engines, this list repeats.
    custom_palette = [full_set2[5], full_set2[0], full_set2[1]]
    ax = sns.barplot(
        data=df,
        x="Query",
        y="Time (s)",
        hue="Engine",
        edgecolor="black",
        linewidth=1.0, # Thicker lines help hatches look better
        errorbar=None,
        width=0.6,
        palette=custom_palette
    )
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)
        spine.set_visible(True)
    # --- Apply Hatch Patterns ---
    # Each 'container' corresponds to one Engine's set of bars
    for i, container in enumerate(ax.containers):
        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        for bar in container:
            bar.set_hatch(hatch)

    # --- Formatting ---
    if log_scale:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(ticker.ScalarFormatter())
        plt.ylabel("Execution Time (s)", fontweight='bold')
        plt.ylim(bottom=min_success_time * 0.5)
    else:
        plt.ylabel("Execution Time (s)", fontweight='bold')

    plt.xlabel("Query ID", fontweight='bold')
    # plt.title("Sedona Spatial Benchmark Performance Comparison", fontsize=16, pad=20)

    # Legend update to show hatches
    legend = plt.legend(loc='upper left', frameon=False)

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)

    # --- Annotation Logic ---
    engines_ordered = [t.get_text() for t in legend.get_texts()]
    y_offset = max_success_time * 0.02

    for container_idx, container in enumerate(ax.containers):
        engine_name = engines_ordered[container_idx]
        for bar in container:
            x = bar.get_x() + bar.get_width() / 2
            if pd.isna(x): continue
            query_idx = int(round(x))
            xticklabels = ax.get_xticklabels()

            if 0 <= query_idx < len(xticklabels):
                query_name = xticklabels[query_idx].get_text()
                res = df[(df['Engine'] == engine_name) & (df['Query'] == query_name)]
                if res.empty: continue

                status = res.iloc[0]['Status']
                time_val = res.iloc[0]['Time (s)']

                if status == "success":
                    if not log_scale:
                        ax.text(x, bar.get_height() + y_offset, f'{time_val:.2f}s',
                                ha='center', va='bottom', fontsize=8, rotation=90)
                else:
                    label = status.upper()
                    label_y = y_offset if not log_scale else min_success_time
                    ax.text(x, label_y, label, ha='center', va='bottom',
                            fontsize=8, rotation=90, color='red', fontweight='bold')

    plt.tight_layout()

    if output_file:
        print(f"Saving to {output_file}...")
        plt.savefig(output_file, dpi=300)
    else:
        plt.show()
# Q2, Q4, Q6, Q9, Q10, Q11
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=str, required=True)
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--log-scale", action="store_true")
    parser.add_argument("--queries", type=str)
    args = parser.parse_args()

    q_filter = [q.strip().lower() for q in args.queries.split(',')] if args.queries else None
    df = load_data_to_df(args.results_dir, q_filter)
    plot_benchmark(df, args.output, args.log_scale)

if __name__ == "__main__":
    main()