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

    # \s* makes it completely immune to leading spaces, blank lines, or terminal formatting.
    plans = re.split(r'\s*QUERY PLAN\s*\n-+\n', log_text)

    if len(plans) < 3:
        print(
            f"Warning: {os.path.basename(filepath)} does not contain exactly two 'QUERY PLAN' blocks. Found {len(plans) - 1}. Skipping.")
        return None

    full_plan = plans[1]
    filter_plan = plans[2]

    # --- Extract Metrics from Full Query ---
    total_time = extract_time(r'Execution Time:\s*([\d.]+)', full_plan)

    # Finds the outermost node executing the spatial math (includes Bitmap Heap Scan for Q2)
    full_join_time = extract_time(
        r'(?:Nested Loop|Hash Join|Bitmap Heap Scan|Custom Scan).*?actual time=[\d.]+\.\.([\d.]+)', full_plan)

    # Finds the first base table scan
    scan_time = extract_time(r'(?:Seq Scan|Index Scan).*?actual time=[\d.]+\.\.([\d.]+)', full_plan)
    io_time = extract_io_time(full_plan)

    # --- Extract Metrics from Filter-Only Query ---
    filter_join_time = extract_time(
        r'(?:Nested Loop|Hash Join|Bitmap Heap Scan|Custom Scan).*?actual time=[\d.]+\.\.([\d.]+)', filter_plan)
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
        "Scanning": scanning,
        "Filter Stage": filter_stage,
        "Refinement Stage": refinement_stage,
        "Miscs": miscs
    }


def parse_folder_logs(log_folder, target_sf, as_percentage=True):
    """Iterates through the folder, parses files, and formats the DataFrame."""
    all_queries_data = {}
    search_path = os.path.join(log_folder, f'pgstrom_sf{target_sf}_q*_results.log')

    for filepath in glob.glob(search_path):
        filename = os.path.basename(filepath)
        match = FILENAME_PATTERN.search(filename)
        if not match: continue

        query_id = f"Q{match.group(2)}"

        parsed_data = parse_raw_postgis_file(filepath)
        print(query_id, parsed_data)
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

    # Toggle between relative percentage and absolute running time
    if as_percentage:
        df_out = df.div(df.sum(axis=1), axis=0) * 100
    else:
        df_out = df

    return df_out[COLUMNS_ORDER]


def draw_subplot(ax, df, palette, is_percentage=True, show_ylabel=True):
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

    if is_percentage:
        ax.set_xlim(0, 100)

    ax.set_ylabel("Query", fontweight='bold') if show_ylabel else ax.set_ylabel('')
    ax.set_axisbelow(True)
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)


def create_figure(df, output_filename):
    """Creates the final single-plot figure."""
    num_queries = len(df) if not df.empty else 0
    height = max(3.5, num_queries * 0.4)

    # Create a single axis figure (width adjusted down slightly since there's only one plot)
    fig, ax = plt.subplots(figsize=(8, height))

    full_set2 = sns.color_palette("Set2")
    custom_palette = [full_set2[0], full_set2[1], full_set2[2], full_set2[5], full_set2[3]]

    draw_subplot(ax, df, custom_palette, is_percentage=True, show_ylabel=True)

    ax.set_xlabel("Proportion of Execution Time (%)", fontweight='bold')

    legend_handles = [
        Patch(facecolor=custom_palette[i], hatch=HATCH_PATTERNS[i], label=COLUMNS_ORDER[i], edgecolor='black')
        for i in range(len(COLUMNS_ORDER))
    ]

    leg = fig.legend(handles=legend_handles, loc='lower center', bbox_to_anchor=(0.5, 0.92), ncol=5, frameon=False,
                     fontsize=11)

    plt.tight_layout()
    fig.subplots_adjust(top=0.85)  # Make room for the legend at the top

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

    print(f"Loading results from: {log_folder}")
    # Defaulting to SF 1 as per the original script, change target_sf if needed
    df = parse_folder_logs(log_folder, target_sf=1, as_percentage=True)

    if df.empty:
        print("Error: No data found.")
        return

    print("\n--- Breakdown (%) ---")
    print(df.round(2).to_string())

    create_figure(df, args.output)


if __name__ == "__main__":
    main()
