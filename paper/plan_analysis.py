import re
import matplotlib.pyplot as plt
import numpy as np
import argparse
import sys


def parse_time(time_str, unit):
    """Converts the extracted time string into seconds."""
    val = float(time_str)
    if unit == 'ms':
        return val / 1000.0
    elif unit == 'µs':
        return val / 1000000.0
    return val  # Already in seconds


def extract_metrics(file_path):
    """Parses a log file, isolates the last run, and extracts the execution times in seconds."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            log_text = file.read()
    except FileNotFoundError:
        print(f"Error: Could not find '{file_path}'. Please check the path and try again.")
        sys.exit(1)

    # Isolate the final run by splitting the log at "Run # X" markers
    # We take the last element of the resulting list (-1), which contains the final run + the average total time
    run_blocks = re.split(r'Run # \d+', log_text)
    target_text = run_blocks[-1] if len(run_blocks) > 1 else log_text

    # 1. Zone Scan Time (I/O & Parquet Decoding)
    zone_match = re.search(r"zone/zone.*?time_elapsed_scanning_total=([\d\.]+)(ms|s|µs)", target_text, re.DOTALL)
    zone_time = parse_time(zone_match.group(1), zone_match.group(2)) if zone_match else 0.0

    # 2. Trip Scan Time (I/O & Parquet Decoding)
    trip_match = re.search(r"trip/trip.*?time_elapsed_scanning_total=([\d\.]+)(ms|s|µs)", target_text, re.DOTALL)
    trip_time = parse_time(trip_match.group(1), trip_match.group(2)) if trip_match else 0.0

    # 3. Spatial Join Time (Compute)
    join_match = re.search(r"SpatialJoinExec.*?join_time=([\d\.]+)(ms|s|µs)", target_text, re.DOTALL)
    join_time = parse_time(join_match.group(1), join_match.group(2)) if join_match else 0.0

    # 4. Total Execution Time (Looks at the very end of the file)
    total_match = re.search(r"Avg execution time \([^)]+\): ([\d\.]+)s", target_text)
    total_time = float(total_match.group(1)) if total_match else 0.0

    return [zone_time, trip_time, join_time, total_time]


def generate_comparison_chart(cpu_file, gpu_file, output_file):
    """Generates a side-by-side bar chart comparing CPU and GPU times."""
    cpu_metrics = extract_metrics(cpu_file)
    gpu_metrics = extract_metrics(gpu_file)

    labels = ['Zone Data Scan (I/O)', 'Trip Data Scan (I/O)', 'Spatial Join (Compute)', 'Total Execution Time']

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))

    rects1 = ax.bar(x - width / 2, cpu_metrics, width, label='CPU', color='#d62728')
    rects2 = ax.bar(x + width / 2, gpu_metrics, width, label='GPU', color='#1f77b4')

    ax.set_ylabel('Time (Seconds)', fontsize=12)
    ax.set_title('CPU vs GPU Spatial Query Execution Breakdown (Final Run)', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)
    ax.legend(fontsize=12)
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{height:.2f}s',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=10)

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()
    plt.savefig(output_file, dpi=300)
    print(f"Comparison chart generated successfully and saved as '{output_file}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare CPU and GPU execution logs (last run only) and generate a bar chart.")
    parser.add_argument("cpu_log", help="Path to the CPU execution log file")
    parser.add_argument("gpu_log", help="Path to the GPU execution log file")
    parser.add_argument("-o", "--output", default="cpu_vs_gpu_final_run.png",
                        help="Path for the output image file (default: cpu_vs_gpu_final_run.png)")

    args = parser.parse_args()

    generate_comparison_chart(args.cpu_log, args.gpu_log, args.output)
