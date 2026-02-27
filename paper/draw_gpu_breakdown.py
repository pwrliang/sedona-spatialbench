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

# Define a list of hatch patterns for high readability
HATCH_PATTERNS = ['/', '\\', '.', 'x']

# --- Data Configuration ---
FILENAME_PATTERN = re.compile(r'sedonadb_gpu_(q\d+)_results\.log')
TIMING_PATTERN = re.compile(r'([A-Za-z\s]+):\s*([\d.]+)\s*ms')

INDEX_MAPPING = {
    "PushBuild": "Build Spatial Index",
    "FinishBuilding": "Build Spatial Index",
    "Alloc": "Filtering",
    "Prepare": "Filtering",
    "RT": "Filtering",
    "Copy res": "Filtering"
}

REFINER_MAPPING = {
    "PushBuild": "WKB to GPU-format",
    "FinishBuilding": "WKB to GPU-format",
    "Parse": "Refinement",
    "Refine": "Refinement",
    "Copy Results": "Refinement"
}

COLUMNS_ORDER = [
    "Build Spatial Index",
    "Filtering",
    "WKB to GPU-format",
    "Refinement"
]


def parse_folder_logs(log_folder):
    """Parses log files, aggregates data into macro-components, and returns proportions."""
    all_queries_data = {}
    search_path = os.path.join(log_folder, 'sedonadb_gpu_*_results.log')

    for filepath in glob.glob(search_path):
        filename = os.path.basename(filepath)
        match = FILENAME_PATTERN.search(filename)
        if not match: continue

        query_id = match.group(1).upper()
        query_times = {}

        try:
            with open(filepath, 'r') as file:
                for line in file:
                    if "Profiling Results." in line:
                        if "RTSpatialIndex" in line:
                            mapping = INDEX_MAPPING
                        elif "RTSpatialRefiner" in line:
                            mapping = REFINER_MAPPING
                        else:
                            continue

                        profiling_str = line.split("Profiling Results.")[1].strip()
                        pairs = TIMING_PATTERN.findall(profiling_str)
                        for key, val in pairs:
                            raw_key = key.strip()
                            if raw_key in mapping:
                                component_name = mapping[raw_key]
                                query_times[component_name] = query_times.get(component_name, 0.0) + float(val)
        except Exception as e:
            print(f"Warning: Could not process {filepath}: {e}")
            continue

        if query_times:
            all_queries_data[query_id] = query_times

    if not all_queries_data:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(all_queries_data, orient='index').fillna(0)

    # Sort index logically (Q1, Q2, ..., Q10)
    df.index = pd.CategoricalIndex(
        df.index,
        categories=sorted(df.index, key=lambda x: int(x[1:])),
        ordered=True
    )
    df = df.sort_index()

    # Convert absolute times to percentage proportions
    df_portions = df.div(df.sum(axis=1), axis=0) * 100

    # Keep only defined columns in the correct order
    present_columns = [col for col in COLUMNS_ORDER if col in df_portions.columns]
    return df_portions[present_columns]


def draw_subplot(ax, df, palette, show_ylabel=True):
    """Helper function to draw horizontal stacked bars on a specific axes."""
    if df.empty:
        ax.text(0.5, 0.5, "No Data", ha='center', va='center', transform=ax.transAxes)
        return

    # Draw Barplot
    df.plot(
        kind='barh',
        stacked=True,
        ax=ax,
        color=palette,
        legend=False,
        width=0.7,
        edgecolor='black',
        linewidth=1.0
    )

    # Styling Spines
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)
        spine.set_visible(True)

    # Apply Hatches
    for i, bar in enumerate(ax.patches):
        component_idx = i // len(df)
        hatch = HATCH_PATTERNS[component_idx % len(HATCH_PATTERNS)]
        bar.set_hatch(hatch)

    # Axis Formatting
    ax.set_xlim(0, 100)

    if show_ylabel:
        ax.set_ylabel("Query", fontweight='bold')
    else:
        ax.set_ylabel('')

    # Grid (X-axis for horizontal bars)
    ax.set_axisbelow(True)
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)


def create_figure(df_sf1, df_sf10, output_filename):
    """Creates the final multi-subplot figure side-by-side."""
    num_queries = max(len(df_sf1) if not df_sf1.empty else 0, len(df_sf10) if not df_sf10.empty else 0)
    height = max(3.0, num_queries * 0.4)

    # 1x2 layout, sharing the Y-axis so the inner spine is clean
    # Increased width slightly to 12 to perfectly match a 2-column spread or wide 1-column layout
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, height), sharey=True)

    full_set2 = sns.color_palette("Set2")
    custom_palette = [full_set2[0], full_set2[1], full_set2[2], full_set2[5]]

    # Draw the subplots without top titles
    draw_subplot(ax1, df_sf1, custom_palette, show_ylabel=True)
    draw_subplot(ax2, df_sf10, custom_palette, show_ylabel=False)

    # Explicitly set the X labels on both charts, using \n\n to place the title below the axis label
    ax1.set_xlabel("Proportion of Spatial Join Time (%)\n\n(a) Scale Factor: 1", fontweight='bold')
    ax2.set_xlabel("Proportion of Spatial Join Time (%)\n\n(b) Scale Factor: 10", fontweight='bold')

    legend_handles = [
        Patch(facecolor=custom_palette[i], hatch=HATCH_PATTERNS[i], label=COLUMNS_ORDER[i], edgecolor='black',
              linewidth=1.0)
        for i in range(len(COLUMNS_ORDER))
    ]

    # Change legend to perfectly span across the top horizontally (4 columns)
    # MUST save to a variable `leg` to prevent cropping
    leg = fig.legend(
        handles=legend_handles,
        loc='lower center',
        bbox_to_anchor=(0.5, 0.9),
        ncol=4,
        frameon=False,
        edgecolor='silver',
        fontsize=13
    )

    plt.tight_layout()
    # Bring the two charts closer with wspace, and leave ample top margin for legend
    fig.subplots_adjust(top=0.88, wspace=0.1)

    print(f"\nSaving to {output_filename}...")

    # Critical fix: explicitly tell savefig to not crop out the 'leg' object!
    plt.savefig(output_filename, bbox_inches='tight', bbox_extra_artists=(leg,))
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Generate spatial join execution time breakdown figures.")
    parser.add_argument("--sf1", type=str, required=True, help="Path to the directory containing SF1 log files.")
    parser.add_argument("--sf10", type=str, required=True, help="Path to the directory containing SF10 log files.")
    parser.add_argument("--output", type=str, default="time_breakdown_side_by_side.pdf",
                        help="Filename for the saved plot.")
    args = parser.parse_args()

    print(f"Loading SF1 results from: {args.sf1}")
    df_sf1 = parse_folder_logs(args.sf1)

    print(f"Loading SF10 results from: {args.sf10}")
    df_sf10 = parse_folder_logs(args.sf10)

    if df_sf1.empty and df_sf10.empty:
        print("Error: No data found in either directory.")
        return

    # Print the calculated percentages to the console
    if not df_sf1.empty:
        print("\n" + "=" * 50)
        print("SF1 Execution Time Breakdown (%)")
        print("=" * 50)
        print(df_sf1.round(2).to_string())

    if not df_sf10.empty:
        print("\n" + "=" * 50)
        print("SF10 Execution Time Breakdown (%)")
        print("=" * 50)
        print(df_sf10.round(2).to_string())

    create_figure(df_sf1, df_sf10, args.output)


if __name__ == "__main__":
    main()
