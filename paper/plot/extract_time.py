import json
import statistics
import argparse
import sys


def main():
    # Set up the command line argument parser
    parser = argparse.ArgumentParser(
        description="Extract average running time and standard deviation from benchmark logs.")
    parser.add_argument("log_path", help="Path to the JSON log file")
    args = parser.parse_args()

    # Read and parse the JSON file
    try:
        with open(args.log_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print(f"Error: The file '{args.log_path}' was not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: The file '{args.log_path}' contains invalid JSON.")
        sys.exit(1)

    # Access the query results based on the provided JSON structure
    try:
        engine_results = data["results"][0]
        query_results = engine_results["results"]
    except (KeyError, IndexError):
        print("Error: The JSON structure does not match the expected format.")
        sys.exit(1)

    # Print the table header
    print(f"{'Query':<8} | {'Avg (Std Dev) in seconds'}")
    print("-" * 35)

    # Calculate and print metrics for each query
    for q in query_results:
        query_name = q.get("query", "Unknown")
        raw_times = q.get("raw_times", [])

        if not raw_times:
            print(f"{query_name:<8} | N/A")
            continue

        # Calculate average
        avg_time = statistics.mean(raw_times)

        # Calculate standard deviation (requires at least 2 data points)
        if len(raw_times) > 1:
            stdev_time = statistics.stdev(raw_times)
        else:
            stdev_time = 0.0

        # Print the formatted result string
        result_str = f"{avg_time:.4f} ({stdev_time:.4f})"
        print(f"{query_name:<8} | {result_str}")


if __name__ == "__main__":
    main()
