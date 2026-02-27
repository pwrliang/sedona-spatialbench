#!/usr/bin/env python3
import re
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
import sys

# --- Configuration for Single-Column Research Figures ---
# Font scale adjusted to balance side-by-side charts in a single column
sns.set_theme(style="whitegrid", context="paper", font_scale=1.3)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['savefig.bbox'] = 'tight'

# Define a list of hatch patterns for high readability
HATCH_PATTERNS = ['//', '\\\\', '..', 'o', '*', 'x']


def parse_time(time_str, unit):
    """Converts the extracted time string into seconds."""
    val = float(time_str)
    if unit == 'ms':
        return val / 1000.0
    elif unit == 'µs':
        return val / 1000000.0
    return val


def extract_metrics(file_path):
    """Parses a log file, isolates the last run, unwraps the table, and extracts times."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            log_text = file.read()
    except FileNotFoundError:
        print(f"Error: Could not find '{file_path}'. Please check the path and try again.")
        sys.exit(1)

    # 1. Isolate the final run
    run_blocks = re.split(r'Run # \d+', log_text)
    target_text = run_blocks[-1] if len(run_blocks) > 1 else log_text

    # 2. CLEAN TEXT: Powerfully strip all newlines, spaces, and table borders
    clean_text = re.sub(r'[│┆\n\s]+', '', target_text)

    # 3. Extract metrics using the stitched text
    zone_match = re.search(r"zone/zone.*?time_elapsed_scanning_total=([\d\.]+)(ms|s|µs)", clean_text)
    zone_time = parse_time(zone_match.group(1), zone_match.group(2)) if zone_match else 0.0

    trip_match = re.search(r"trip/trip.*?time_elapsed_scanning_total=([\d\.]+)(ms|s|µs)", clean_text)
    trip_time = parse_time(trip_match.group(1), trip_match.group(2)) if trip_match else 0.0

    join_match = re.search(r"SpatialJoinExec.*?join_time=([\d\.]+)(ms|s|µs)", clean_text)
    join_time = parse_time(join_match.group(1), join_match.group(2)) if join_match else 0.0

    total_match = re.search(r"Avgexecutiontime\([^)]+\):([\d\.]+)s", clean_text)
    total_time = float(total_match.group(1)) if total_match else 0.0

    print(f"--- Parsed Metrics from {file_path} ---")
    print(
        f"Zone Scan: {zone_time:.4f}s | Trip Scan: {trip_time:.4f}s | Spatial Join: {join_time:.4f}s | Total: {total_time:.4f}s")

    return {
        'Zone Table Scan': zone_time,
        'Trip Table Scan': trip_time,
        'Spatial Join': join_time,
        'Total': total_time
    }


def draw_subplot(ax, cpu_metrics, gpu_metrics, title, show_ylabel=True):
    """Helper function to draw a stacked bar chart on a specific axes."""
    components = ['Zone Table Scan', 'Trip Table Scan', 'Spatial Join']
    cpu_vals = [cpu_metrics[comp] for comp in components]
    gpu_vals = [gpu_metrics[comp] for comp in components]

    x_labels = ['CPU', 'GPU']
    width = 0.5
    palette = sns.color_palette("Set2")
    bottoms = np.zeros(2)

    max_total = max(cpu_metrics['Total'], gpu_metrics['Total'])

    for i, comp_name in enumerate(components):
        vals = [cpu_vals[i], gpu_vals[i]]

        bars = ax.bar(x_labels, vals, width, label=comp_name, bottom=bottoms,
                      color=palette[i], edgecolor="black", linewidth=1.2,
                      hatch=HATCH_PATTERNS[i % len(HATCH_PATTERNS)])

        # Add labels inside the stacked segments using a dynamic threshold
        for j, bar in enumerate(bars):
            h = bar.get_height()
            if h > (max_total * 0.05):  # Only show text if segment is > 5% of max total
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + h / 2,
                        f'{h:.2f}s', ha='center', va='center', fontsize=10,
                        color='black', fontweight='bold',
                        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=0.3))

        bottoms += vals

    # Apply professional styling to Spines
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)
        spine.set_visible(True)

    # Formatting axes and setting the title at the bottom
    if show_ylabel:
        ax.set_ylabel("Execution Time (s)", fontweight='bold')

    ax.set_xlabel(title, fontweight='bold', labelpad=10)

    # Grid Control: Keep horizontal (y), explicitly disable vertical (x)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)


def generate_dual_chart(args):
    """Generates a side-by-side figure suitable for a single column."""
    print(f"Loading data for {args.q1_name}...")
    cpu1 = extract_metrics(args.q1_cpu)
    gpu1 = extract_metrics(args.q1_gpu)

    print(f"\nLoading data for {args.q2_name}...")
    cpu2 = extract_metrics(args.q2_cpu)
    gpu2 = extract_metrics(args.q2_gpu)

    # 1x2 layout, shared Y-axis to save horizontal space.
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 4.0), sharey=True)

    # Draw subplots. Only show the Y-axis label on the left-most chart.
    draw_subplot(ax1, cpu1, gpu1, f"(a) {args.q1_name}: CPU vs GPU", show_ylabel=True)
    draw_subplot(ax2, cpu2, gpu2, f"(b) {args.q2_name}: CPU vs GPU", show_ylabel=False)

    # Extract handles from the first axis to create a global, shared legend
    handles, labels = ax1.get_legend_handles_labels()

    # Pack the legend tightly into 3 columns at the top
    fig.legend(handles, labels, loc='lower center', bbox_to_anchor=(0.5, 0.82),
               ncol=3, frameon=False, edgecolor='black',
               fontsize=14, columnspacing=0.8, handletextpad=0.4, handlelength=1.2)

    # Compress layout, adjust top margin for the legend, and bring subplots closer
    plt.tight_layout()
    fig.subplots_adjust(top=0.85, wspace=0.1)

    plt.savefig(args.output, format='pdf', dpi=300)
    print(f"\nSingle-column side-by-side chart successfully saved to {args.output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a side-by-side stacked bar chart for two queries.")

    # Query 1 Arguments
    parser.add_argument("--q1_name", default="Q2", help="Display name for the first query (e.g., Q2)")
    parser.add_argument("--q1_cpu", required=True, help="Path to Query 1 CPU log")
    parser.add_argument("--q1_gpu", required=True, help="Path to Query 1 GPU log")

    # Query 2 Arguments
    parser.add_argument("--q2_name", default="Q10", help="Display name for the second query (e.g., Q10)")
    parser.add_argument("--q2_cpu", required=True, help="Path to Query 2 CPU log")
    parser.add_argument("--q2_gpu", required=True, help="Path to Query 2 GPU log")

    # Output Argument
    parser.add_argument("-o", "--output", default="q2_vs_q10_side_by_side.pdf",
                        help="Path for output PDF file")

    args = parser.parse_args()

    generate_dual_chart(args)
