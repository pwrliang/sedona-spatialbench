#!/usr/bin/env python3
import os
import glob
import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import seaborn as sns

# --- Configuration for Academic Figures ---
sns.set_theme(style="whitegrid", context="paper", font_scale=1.5)
plt.rcParams['font.family'] = 'sans-serif'

HATCH_PATTERNS = ['/', '\\', '.', 'x', '+']

COLUMNS_ORDER = [
    "Loading",
    "Scanning",
    "Filter Stage",
    "Refinement Stage",
    "Miscs"
]

FILENAME_PATTERN = re.compile(r'pgstrom_sf(\d+)_q(\d+)_results\.log')


def extract_time(pattern, text):
    """Helper to extract the 'actual time' upper bound from a PostgreSQL EXPLAIN node."""
    match = re.search(pattern, text)
    if match:
        return float(match.group(1))
    return 0.0


def extract_io_time(text):
    """Helper to find all I/O read times and sum them up."""
    matches = re.findall(r'I/O Timings:.*?read=([\d.]+)', text)
    return sum(float(m) for m in matches)


def parse_raw_postgis_file(filepath):
    """Parses a raw EXPLAIN ANALYZE text file containing both the Full and Filter queries."""
    with open(filepath, 'r') as file:
        log_text = file.read()

    # FIX: Split the file using the exact Postgres table header instead of the word "EXPLAIN".
    # This completely ignores bash prompts (spatialbench=#), SQL syntax, and terminal noise.
    plans = re.split(r'QUERY PLAN\n-+\n', log_text)

    # We expect 3 chunks: [0] = Intro/SQL, [1] = Full Plan + 2nd SQL, [2] = Filter Plan
    if len(plans) < 3:
        print(f"Warning: {os.path.basename(filepath)} does not contain exactly two 'QUERY PLAN' blocks. Skipping.")
        return None

    full_plan = plans[1]
    filter_plan = plans[2]

    # --- Extract Metrics from Full Query ---
    total_time = extract_time(r'Execution Time:\s*([\d.]+)', full_plan)
    # Finds the outermost Nested Loop/Hash Join
    full_join_time = extract_time(r'(?:Nested Loop|Hash Join).*?actual time=[\d.]+\.\.([\d.]+)', full_plan)
    # Finds the first base table scan
    scan_time = extract_time(r'(?:Seq Scan|Index Scan).*?actual time=[\d.]+\.\.([\d.]+)', full_plan)
    io_time = extract_io_time(full_plan)

    # --- Extract Metrics from Filter-Only Query ---
    filter_join_time = extract_time(r'(?:Nested Loop|Hash Join).*?actual time=[\d.]+\.\.([\d.]+)', filter_plan)
    filter_scan_time = extract_time(r'(?:Seq Scan|Index Scan).*?actual time=[\d.]+\.\.([\d.]+)', filter_plan)

    # --- Calculate the Breakdown ---
    loading = io_time
    scanning = max(0.0, scan_time - loading)

    # Filter time is the filter join minus its own scan time
    filter_stage = max(0.0, filter_join_time - filter_scan_time)

    # Refinement is the difference between the full math join and the bounding-box join
    refinement_stage = max(0.0, full_join_time - filter_join_time)

    # Miscs is whatever is left over at the top of the tree (Aggregations/Sorts)
    miscs = max(0.0, total_time - full_join_time)

    return {
        "Loading": loading,
        "Scanning": scanning,
        "Filter Stage": filter_stage,
        "Refinement Stage": refinement_stage,
        "Miscs": miscs
    }

def parse_folder_logs(log_folder, target_sf):
    """Iterates through the folder, parses files, and formats the DataFrame."""
    all_queries_data = {}
    search_path = os.path.join(log_folder, f'pgstrom_sf{target_sf}_q*_results.log')

    for filepath in glob.glob(search_path):
        filename = os.path.basename(filepath)
        match = FILENAME_PATTERN.search(filename)
        if not match: continue

        query_id = f"Q{match.group(2)}"

        parsed_data = parse_raw_postgis_file(filepath)
        if parsed_data:
            all_queries_data[query_id] = parsed_data

    if not all_queries_data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(all_queries_data, orient='index').fillna(0)

    # Sort index logically (Q1, Q2, ..., Q11)
    df.index = pd.CategoricalIndex(
        df.index,
        categories=sorted(df.index, key=lambda x: int(x[1:])),
        ordered=True
    )
    df = df.sort_index()

    # Convert absolute times to percentage proportions
    df_portions = df.div(df.sum(axis=1), axis=0) * 100

    return df_portions[COLUMNS_ORDER]


def draw_subplot(ax, df, palette, show_ylabel=True):
    """Helper function to draw horizontal stacked bars on a specific axes."""
    if df.empty:
        ax.text(0.5, 0.5, "No Data", ha='center', va='center', transform=ax.transAxes)
        return

    df.plot(kind='barh', stacked=True, ax=ax, color=palette, legend=False, width=0.7, edgecolor='black', linewidth=1.0)

    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)

    for i, bar in enumerate(ax.patches):
        component_idx = i // len(df)
        bar.set_hatch(HATCH_PATTERNS[component_idx % len(HATCH_PATTERNS)])

    ax.set_xlim(0, 100)
    ax.set_ylabel("Query", fontweight='bold') if show_ylabel else ax.set_ylabel('')
    ax.set_axisbelow(True)
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)


def create_figure(df_sf1, df_sf10, output_filename):
    """Creates the final multi-subplot figure."""
    num_queries = max(len(df_sf1) if not df_sf1.empty else 0, len(df_sf10) if not df_sf10.empty else 0)
    height = max(3.5, num_queries * 0.4)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, height), sharey=True)

    full_set2 = sns.color_palette("Set2")
    custom_palette = [full_set2[0], full_set2[1], full_set2[2], full_set2[5], full_set2[3]]

    draw_subplot(ax1, df_sf1, custom_palette, show_ylabel=True)
    draw_subplot(ax2, df_sf10, custom_palette, show_ylabel=False)

    ax1.set_xlabel("Proportion of Execution Time (%)\n\n(a) Scale Factor: 1", fontweight='bold')
    ax2.set_xlabel("Proportion of Execution Time (%)\n\n(b) Scale Factor: 10", fontweight='bold')

    legend_handles = [
        Patch(facecolor=custom_palette[i], hatch=HATCH_PATTERNS[i], label=COLUMNS_ORDER[i], edgecolor='black')
        for i in range(len(COLUMNS_ORDER))
    ]

    leg = fig.legend(handles=legend_handles, loc='lower center', bbox_to_anchor=(0.5, 0.9), ncol=5, frameon=False,
                     fontsize=11)

    plt.tight_layout()
    fig.subplots_adjust(top=0.88, wspace=0.1)

    if output_filename:
        print(f"\nSaving to {output_filename}...")
        plt.savefig(output_filename, bbox_inches='tight', bbox_extra_artists=(leg,))
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description="Parse raw EXPLAIN logs and generate breakdown figures.")
    parser.add_argument("log_dir", type=str, nargs='+', help="Path to the directory containing raw log files.")
    parser.add_argument("--output", type=str, default=None, help="Filename for the saved plot.")
    args = parser.parse_args()

    log_folder = args.log_dir[0]

    print(f"Loading SF1 results from: {log_folder}")
    df_sf1 = parse_folder_logs(log_folder, target_sf=1)

    print(f"Loading SF10 results from: {log_folder}")
    df_sf10 = parse_folder_logs(log_folder, target_sf=10)

    if df_sf1.empty and df_sf10.empty:
        print("Error: No data found.")
        return

    if not df_sf1.empty:
        print("\n--- SF1 Breakdown (%) ---")
        print(df_sf1.round(2).to_string())

    if not df_sf10.empty:
        print("\n--- SF10 Breakdown (%) ---")
        print(df_sf10.round(2).to_string())

    create_figure(df_sf1, df_sf10, args.output)


if __name__ == "__main__":
    main()
