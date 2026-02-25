import os
import json
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# --- Configuration for Research Quality Figures ---
sns.set_theme(style="whitegrid", context="paper", font_scale=2.5)
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['savefig.bbox'] = 'tight'

# Hardcoded mapping from raw directory names to clean paper-ready GPU labels
DEVICE_LABEL_MAP = {
    "g6e.2xlarge": "L40S",
    "g5.2xlarge": "A10",
    "g6.2xlarge": "L4"
}


def apply_professional_styling(ax, subfigure_title, use_log_y):
    """Helper function to apply consistent styling to axes."""
    # Styling Spines
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.2)
        spine.set_visible(True)

    # Grid styling
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)

    # Log scale formatting if enabled
    if use_log_y:
        ax.set_yscale('log')
        # Format the log ticks as normal numbers (e.g., 1, 2, 3) instead of 10^0
        formatter = ticker.ScalarFormatter()
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)
        ax.yaxis.set_minor_formatter(formatter)

    # Label styling - appending the subfigure title beneath the x-axis label
    ax.set_xlabel(f'Number of Cores\n\n{subfigure_title}', fontweight='bold')
    ax.set_ylabel('Execution Time (s)' + (' (Log Scale)' if use_log_y else ''), fontweight='bold')

    # Legend
    ax.legend(loc='best', frameon=False, fontsize=16, title="GPU")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results for devices matching prefixes.")
    parser.add_argument("root_dir", help="Root directory containing the device logs (e.g., 'logs/')")
    parser.add_argument("--prefix", required=True,
                        help="Comma-separated prefixes of the device directories to include (e.g., 'g6e,g6')")
    parser.add_argument("--output", default="benchmark_scaling_comparison.pdf", help="Output image filename")
    parser.add_argument("--log-y", action="store_true", help="Use a logarithmic scale for the Y-axis")
    args = parser.parse_args()

    max_cores = 8
    scale_factor = 1
    file_name = "sedonadb_gpu_results.json"

    prefixes = tuple(p.strip() for p in args.prefix.split(','))
    all_data = {}

    if not os.path.exists(args.root_dir):
        print(f"Error: Root directory '{args.root_dir}' does not exist.")
        return

    device_dirs = [d for d in os.listdir(args.root_dir)
                   if d.startswith(prefixes) and os.path.isdir(os.path.join(args.root_dir, d))]

    if not device_dirs:
        print(f"No directories found in '{args.root_dir}' starting with prefixes '{args.prefix}'.")
        return

    print(f"Found devices matching '{args.prefix}': {', '.join(device_dirs)}")

    # Parse the JSON files
    for device_name in device_dirs:
        log_dir = os.path.join(args.root_dir, device_name)

        cores = []
        q2_times = []
        q11_times = []

        for core in range(1, max_cores + 1):
            folder_name = f"results_SF_{scale_factor}_CPU_LIMIT_{core}"
            file_path = os.path.join(log_dir, folder_name, file_name)

            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    try:
                        data = json.load(f)
                        query_results = data.get("results", [])[0].get("results", [])

                        q2_time, q11_time = None, None

                        for q in query_results:
                            if q.get("query") == "q2":
                                q2_time = q.get("time_seconds")
                            elif q.get("query") == "q11":
                                q11_time = q.get("time_seconds")

                        if q2_time is not None and q11_time is not None:
                            cores.append(core)
                            q2_times.append(q2_time)
                            q11_times.append(q11_time)
                    except (json.JSONDecodeError, IndexError, AttributeError):
                        pass

        if cores:
            display_name = DEVICE_LABEL_MAP.get(device_name, device_name)
            all_data[display_name] = {'cores': cores, 'q2': q2_times, 'q11': q11_times}

    if not all_data:
        print("No valid data found to plot.")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7.5))

    color_palette = sns.color_palette("Set2")
    markers = ['o', 's', '^', 'D', 'v', 'p', '*', 'X']
    linestyles = ['-', '--', '-.', ':']

    # Sort the display names so the legend is consistent
    for i, display_name in enumerate(sorted(all_data.keys())):
        data = all_data[display_name]

        # Cycle through properties
        color = color_palette[i % len(color_palette)]
        marker = markers[i % len(markers)]
        linestyle = linestyles[i % 2]  # Alternates between solid and dashed

        # Make alternate markers hollow (white face) so overlaps show through
        markerfacecolor = 'white' if i % 2 != 0 else color
        markeredgecolor = color
        markeredgewidth = 2.5 if i % 2 != 0 else 1

        # Left plot: Q2
        ax1.plot(data['cores'], data['q2'],
                 marker=marker, markersize=12, linewidth=3.5,
                 linestyle=linestyle, color=color,
                 markerfacecolor=markerfacecolor, markeredgecolor=markeredgecolor, markeredgewidth=markeredgewidth,
                 label=display_name)

        # Right plot: Q11
        ax2.plot(data['cores'], data['q11'],
                 marker=marker, markersize=12, linewidth=3.5,
                 linestyle=linestyle, color=color,
                 markerfacecolor=markerfacecolor, markeredgecolor=markeredgecolor, markeredgewidth=markeredgewidth,
                 label=display_name)

    # Apply styling
    apply_professional_styling(ax1, "(a) Q2: Execution Time vs CPU Cores", args.log_y)
    ax1.set_xticks(range(1, max_cores + 1))

    apply_professional_styling(ax2, "(b) Q11: Execution Time vs CPU Cores", args.log_y)
    ax2.set_xticks(range(1, max_cores + 1))

    plt.tight_layout()
    plt.savefig(args.output, dpi=300)
    print(f"\nPlot successfully saved as '{args.output}'")


if __name__ == "__main__":
    main()
