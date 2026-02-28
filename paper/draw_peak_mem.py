import os
import glob
import re
import matplotlib.pyplot as plt

# 1. Define the folder containing the log files
# Replace this with your actual folder path, or use "." for the current directory
log_folder = "/Users/liang/PycharmProjects/sedona-spatialbench/paper/logs/g6e.2xlarge/results_mem_SF_10"
# File pattern to match all log files in the folder
file_pattern = os.path.join(log_folder, "sedonadb_gpu_q*_quota_*_results.log")

# Dictionary to hold our parsed data.
# Structure: { 'Query 2': [(quota, avg_mem), ...], 'Query 11': [(quota, avg_mem), ...] }
data = {}

# Regular expressions to safely extract query number, quota, and memory
query_regex = re.compile(r'_q(\d+)_')
quota_regex = re.compile(r'_quota_(\d+)_')
# Regex to match the specific "PIP(Used X MB), Batch 0" line
memory_regex = re.compile(r'PIP\(Used (\d+) MB\), Batch 0')

# 2. Process each log file
for filepath in glob.glob(file_pattern):
    filename = os.path.basename(filepath)

    # Extract query name and quota from the filename
    query_match = query_regex.search(filename)
    quota_match = quota_regex.search(filename)

    if query_match and quota_match:
        query_name = f"Query {query_match.group(1)}"
        quota = int(quota_match.group(1))

        # Read file to extract memory values
        with open(filepath, 'r') as file:
            content = file.read()

            # Find all instances matching the specific PIP/Batch 0 string
            memory_usages = memory_regex.findall(content)

            # Only process the file if the specific line is found
            if memory_usages:
                # Convert list of strings to integers and calculate the average
                mem_values = [int(mem) for mem in memory_usages]
                avg_memory = sum(mem_values) / len(mem_values)

                # Group the data by query name so each gets its own curve
                if query_name not in data:
                    data[query_name] = []
                data[query_name].append((quota, max(mem_values)))

# 3. Draw the figure
if data:
    plt.figure(figsize=(10, 6))

    # This loop ensures each query gets its own separate curve on the plot
    for query_name, values in data.items():
        # Sort the values by quota (x-axis) so the line draws correctly from left to right
        values.sort(key=lambda x: x[0])

        quotas = [v[0] for v in values]
        avg_mems = [v[1] for v in values]

        # Plot the curve for this specific query
        plt.plot(quotas, avg_mems, marker='o', linestyle='-', linewidth=2, label=query_name)

    # 4. Format the plot
    plt.title('Average Memory vs. Memory Quota')
    plt.xlabel('Memory Quota')
    plt.ylabel('Average Memory (MB)')
    plt.grid(True, linestyle='--', alpha=0.7)

    # Add a legend to show which curve belongs to which query
    plt.legend(title="Queries")
    plt.tight_layout()

    # Save and display the plot
    plt.savefig('average_memory_pip_batch0.png', dpi=300)
    plt.show()
else:
    print("No matching data found in the logs.")
