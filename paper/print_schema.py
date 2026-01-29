import pyarrow.parquet as pq
import os

# Adjust this path if your data folder has a different name
data_dir = "benchmark-data-sf1"

print(f"Scanning directory: {data_dir}\n")

for root, dirs, files in os.walk(data_dir):
    # Sort files to ensure deterministic output
    files.sort()
    for file in files:
        if file.endswith(".parquet"):
            path = os.path.join(root, file)
            print(f"==========================================")
            print(f"File: {os.path.basename(root)}/{file}")
            print(f"==========================================")
            try:
                # Read schema
                schema = pq.read_schema(path)
                for name, type in zip(schema.names, schema.types):
                    # Print exact column name and Arrow type
                    print(f"{name:<20} | {type}")
            except Exception as e:
                print(f"Error reading schema: {e}")

            # We only need to check one file per table/folder
            break
    print("")
