import pandas as pd
import argparse
from pathlib import Path
import re

# --- CONFIGURATION ---
DEVICE_NAME_MAPPING = {
    "g5.2xlarge": "A10",
    "g6.2xlarge": "L4",
    "g6e.2xlarge": "L40S",
    "m7i.2xlarge": "CPU",
    "A100_119_cores": "A100",
    "H100_40_cores": "H100",
    "3090": "RTX3090"
}

# Define the baseline for speedup calculation
BASELINE_DEVICE = "CPU"

# Order for the table output
DEVICE_ORDER = ["L40S", "A10", "L4", "A100", "H100", "RTX3090"]


def map_device_name(column_name):
    for raw_name, clean_name in DEVICE_NAME_MAPPING.items():
        if raw_name.lower() in column_name.lower():
            return clean_name
    return column_name


def pretty_print_speedups(csv_file):
    csv_path = Path(csv_file)
    if not csv_path.exists():
        print(f"Error: {csv_file} not found.")
        return

    # 1. Load Data
    df = pd.read_csv(csv_path)
    sf = re.search(r'SF_(\d+)', csv_path.name).group(1) if re.search(r'SF_(\d+)', csv_path.name) else "Unknown"

    # 2. Process and Map Names
    df_long = pd.melt(df, id_vars=['Query'], var_name='Raw_Device', value_name='Time_Seconds').dropna()
    df_long['Device'] = df_long['Raw_Device'].apply(map_device_name)
    df_long['Query'] = df_long['Query'].str.upper()

    # 3. Pivot for Speedup Calculation
    pivot_df = df_long.pivot(index='Query', columns='Device', values='Time_Seconds')

    if BASELINE_DEVICE not in pivot_df.columns:
        print(f"Error: Baseline '{BASELINE_DEVICE}' not found in the CSV columns.")
        return

    # Natural Sort Queries
    pivot_df['Query_Num'] = pivot_df.index.str.extract(r'(\d+)').astype(int).values
    pivot_df = pivot_df.sort_values(by='Query_Num').drop(columns=['Query_Num'])

    # 4. Calculate Speedup (Baseline Time / Target Time)
    speedup_df = pivot_df.copy()
    for col in pivot_df.columns:
        speedup_df[col] = pivot_df[BASELINE_DEVICE] / pivot_df[col]

    # --- PRETTY PRINTING FOR PAPER REFERENCE ---
    print("\n" + "=" * 75)
    print(f"PERFORMANCE SPEEDUP ANALYSIS (Relative to {BASELINE_DEVICE}, SF {sf})")
    print("=" * 75)

    # Header
    header = f"{'Query':<10}"
    for dev in DEVICE_ORDER:
        if dev in speedup_df.columns:
            header += f" | {dev:>15}"
    print(header)
    print("-" * len(header))

    # Rows (Per Query)
    for query, row in speedup_df.iterrows():
        row_str = f"{query:<10}"
        for dev in DEVICE_ORDER:
            if dev in speedup_df.columns:
                row_str += f" | {row[dev]:>14.2f}x"
        print(row_str)

    print("-" * len(header))

    # Summary (Average Speedup)
    avg_str = f"{'AVERAGE':<10}"
    for dev in DEVICE_ORDER:
        if dev in speedup_df.columns:
            avg_val = speedup_df[dev].mean()
            avg_str += f" | {avg_val:>14.2f}x"
    print(avg_str)
    print("=" * 75 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_file", type=str, required=True)
    args = parser.parse_args()
    pretty_print_speedups(args.csv_file)
