import json
import csv
import re
import argparse
from pathlib import Path
from collections import defaultdict


def process_logs(base_dir, allowed_devices=None, allowed_queries=None):
    # Flatten and clean up arguments in case the user used commas (e.g. "q1,q2" instead of "q1 q2")
    if allowed_devices:
        allowed_devices = {d.strip() for device_str in allowed_devices for d in device_str.split(',')}
    if allowed_queries:
        allowed_queries = {q.strip() for query_str in allowed_queries for q in query_str.split(',')}

    # Nested dictionary to hold data: parsed_data[scale_factor][query][column_title] = time_seconds
    parsed_data = defaultdict(lambda: defaultdict(dict))

    # Track which column titles exist for each scale factor
    columns_per_sf = defaultdict(set)

    base_path = Path(base_dir)

    if not base_path.exists():
        print(f"Error: The log directory '{base_dir}' does not exist.")
        return

    print(f"Scanning directory: {base_path}")
    if allowed_devices:
        print(f"Filtering for devices: {', '.join(allowed_devices)}")
    if allowed_queries:
        print(f"Filtering for queries: {', '.join(allowed_queries)}")

    # 1. Recursively find all JSON files
    found_files = 0
    for json_file in base_path.rglob("*.json"):
        if len(json_file.parts) < 4:
            continue

        device_name = json_file.parts[-3]
        print("device_name", device_name)

        # If the user specified devices, filter based on the folder name
        if allowed_devices and device_name not in allowed_devices:
            continue

        sf_folder = json_file.parts[-2]

        # Ensure it's inside a results_SF_ folder and extract the scale factor
        match = re.search(r'results_SF_(\d+)', sf_folder)
        if not match:
            continue
        sf = match.group(1)

        # 2. Parse the JSON file
        with open(json_file, 'r') as f:
            try:
                data = json.load(f)

                root_results = data.get("results", [])
                if not root_results or not isinstance(root_results, list):
                    continue

                # Extract the engine name from the JSON
                engine_name = root_results[0].get("engine")
                if not engine_name:
                    continue

                # Combine device and engine for the column header
                column_title = f"{device_name}_{engine_name}"
                found_files += 1

                query_results = root_results[0].get("results", [])

                for item in query_results:
                    query = item.get("query")
                    time_sec = item.get("time_seconds")

                    # Apply query filters if specified
                    if allowed_queries and query not in allowed_queries:
                        continue

                    if query and time_sec is not None:
                        parsed_data[sf][query][column_title] = time_sec
                        columns_per_sf[sf].add(column_title)

            except json.JSONDecodeError:
                print(f"Warning: Could not decode JSON in {json_file}")
            except Exception as e:
                print(f"Unexpected error processing {json_file}: {e}")

    if found_files == 0:
        print("No valid log files matching the schema/criteria were found.")
        return

    if not parsed_data:
        print("No queries matching your criteria were found in the logs.")
        return

    # Helper function to sort queries naturally (e.g., q2, q9, q10)
    def query_sort_key(q_string):
        nums = re.findall(r'\d+', q_string)
        return int(nums[0]) if nums else 0

    # 3. Generate a CSV for each scale factor
    for sf, queries_data in parsed_data.items():
        output_filename = f"compiled_results_SF_{sf}.csv"

        # Sort columns alphabetically
        columns = sorted(list(columns_per_sf[sf]))

        # Sort queries logically for consistent rows
        queries = sorted(queries_data.keys(), key=query_sort_key)

        with open(output_filename, 'w', newline='') as f:
            writer = csv.writer(f)

            # Write the header row
            writer.writerow(['Query'] + columns)

            # Write the data rows
            for q in queries:
                row = [q]
                for col in columns:
                    row.append(queries_data[q].get(col, ""))
                writer.writerow(row)

        print(f"Success: Created '{output_filename}' (Queries: {len(queries)}, Columns: {len(columns)})")


# --log-dir /Users/liang/PycharmProjects/sedona-spatialbench/paper/logs   --devices g5.2xlarge,g6.2xlarge,g6e.2xlarge,m7i.2xlarge   --queries q2,q4,q6,q9,q10,q11
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process spatialbench JSON logs into CSV formats.")

    parser.add_argument(
        "--log-dir",
        type=str,
        default="logs",
        help="Path to the base logs directory (default: 'logs')."
    )

    parser.add_argument(
        "--devices",
        type=str,
        nargs='*',
        help="List of specific devices to include. Can be comma-separated or space-separated."
    )

    parser.add_argument(
        "--queries",
        type=str,
        nargs='*',
        help="List of specific queries to include. Can be comma-separated or space-separated."
    )

    args = parser.parse_args()

    process_logs(
        base_dir=args.log_dir,
        allowed_devices=args.devices,
        allowed_queries=args.queries
    )
