import os
import json
import re
import argparse
import matplotlib.pyplot as plt
import seaborn as sns

def style_axis(ax):
    """Applies professional, paper-ready styling to the axis spines with horizontal grid lines."""
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)
        spine.set_visible(True)

    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_dir", help="Path to the logs directory, e.g., logs/g5.2xlarge/spill")
    args = parser.parse_args()

    log_dir = args.log_dir
    data = []

    pattern = re.compile(r"results_SF_\d+_MEM_LIMIT_(\d+)gb")

    for dirname in os.listdir(log_dir):
        match = pattern.search(dirname)
        if match:
            mem_limit = int(match.group(1))
            json_file = os.path.join(log_dir, dirname, "sedonadb_gpu_results.json")
            if os.path.exists(json_file):
                with open(json_file, 'r') as f:
                    content = json.load(f)
                    total_time = content.get("results", [{}])[0].get("total_time")
                    if total_time is not None:
                        data.append((mem_limit, total_time))

    data.sort(key=lambda x: x[0])

    if not data:
        print("No valid data found.")
        return

    mem_limits = [x[0] for x in data]
    times = [x[1] for x in data]

    sns.set_theme(style="white", context="paper", font_scale=2)
    plt.rcParams['font.family'] = 'sans-serif'

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot(mem_limits, times, marker='o', linestyle='-', linewidth=2.5, markersize=10, color=sns.color_palette("Set2")[2], label="Q11")

    ax.set_xlabel('Memory Budget (GB)', fontweight='bold', labelpad=15, fontsize=20)
    ax.set_ylabel('Running Time (s)', fontweight='bold', fontsize=20)
    ax.set_title('Running Time vs Memory Budget', fontweight='bold', fontsize=22, pad=15)

    style_axis(ax)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.legend(frameon=False, fontsize=18)

    plt.tight_layout()
    plt.savefig('spill.pdf', bbox_inches='tight', dpi=300)
    print("Graph saved to spill.pdf")

if __name__ == "__main__":
    main()
