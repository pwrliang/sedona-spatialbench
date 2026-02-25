import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import argparse
from pathlib import Path
import re

# --- CONFIGURATION ---
AWS_HOURLY_PRICES = {
    "m7i.2xlarge": 0.4032,
    "g5.2xlarge": 1.212,
    "g6.2xlarge": 0.9776,
    "g6e.2xlarge": 2.24208
}

DEVICE_NAME_MAPPING = {
    "g5.2xlarge": "A10",
    "g6.2xlarge": "L4",
    "g6e.2xlarge": "L40S",
    "m7i.2xlarge": "CPU"
}

DEVICE_ORDER = ["CPU", "L40S", "A10", "L4"]
HATCH_PATTERNS = ['/', '\\', '.', 'o', '*', 'x', '+', '-', '//']


def get_hourly_price(column_name):
    if 'pgstrom' in column_name: return None
    for device, price in AWS_HOURLY_PRICES.items():
        if device.lower() in column_name.lower(): return price
    return None


def map_device_name(column_name):
    for raw_name, clean_name in DEVICE_NAME_MAPPING.items():
        if raw_name.lower() in column_name.lower(): return clean_name
    return column_name


def generate_combined_cost_figure(csv_file):
    csv_path = Path(csv_file)
    if not csv_path.exists(): return

    # 1. Load and Process Data
    df = pd.read_csv(csv_path)
    sf = re.search(r'SF_(\d+)', csv_path.name).group(1) if re.search(r'SF_(\d+)', csv_path.name) else "Unknown"

    df_long = pd.melt(df, id_vars=['Query'], var_name='Device', value_name='Time_Seconds').dropna(
        subset=['Time_Seconds'])
    df_long['Price_Per_Hour'] = df_long['Device'].apply(get_hourly_price)
    df_long = df_long.dropna(subset=['Price_Per_Hour'])

    # Calculate Cost in USD and transform query names
    df_long['Cost_USD'] = df_long['Time_Seconds'] * (df_long['Price_Per_Hour'] / 3600)
    df_long['Device'] = df_long['Device'].apply(map_device_name)
    df_long['Query'] = df_long['Query'].str.upper()  # Set to uppercase

    # Natural Sorting
    df_long['Query_Num'] = df_long['Query'].str.extract(r'(\d+)').astype(int)
    df_long = df_long.sort_values(by=['Query_Num', 'Device']).drop(columns=['Query_Num'])

    df_total = df_long.groupby('Device')['Cost_USD'].sum().reset_index()

    # --- PLOTTING SETUP ---
    sns.set_theme(style="whitegrid", context="paper", font_scale=1.5)
    custom_palette = [sns.color_palette("Set2")[i] for i in [5, 0, 1, 2]]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), gridspec_kw={'width_ratios': [2, 1]})

    # Custom Formatter to hide "0.0000"
    def hide_zero_formatter(x, pos):
        return f"${x:.4f}" if f"{x:.4f}" != "0.0000" else ""

    # --- PANEL A: Cost Per Query ---
    sns.barplot(data=df_long, x='Query', y='Cost_USD', hue='Device', hue_order=DEVICE_ORDER,
                palette=custom_palette, edgecolor='black', linewidth=1.0, ax=axes[0])

    # Apply Bar Hatches
    for i, container in enumerate(axes[0].containers):
        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        for bar in container: bar.set_hatch(hatch)

    # Move Title to Bottom & Set Log Scale
    axes[0].set_xlabel('(a) Cost per Individual Query', fontweight='bold', labelpad=15, fontsize=20)
    axes[0].set_ylabel('Cost (USD)', fontweight='bold', fontsize=20)
    axes[0].set_yscale('log')
    axes[0].yaxis.set_major_formatter(FuncFormatter(hide_zero_formatter))

    # Fix Legend Hatches: Create, then iterate over patches
    leg = axes[0].legend(loc='lower left', bbox_to_anchor=(0, 1), ncol=4,
                         frameon=False, fontsize=20, markerscale=2)
    for i, patch in enumerate(leg.get_patches()):
        patch.set_hatch(HATCH_PATTERNS[i % len(HATCH_PATTERNS)])
        patch.set_edgecolor('black')

    # --- PANEL B: Total Workload Cost ---
    sns.barplot(data=df_total, x='Device', y='Cost_USD', hue='Device', order=DEVICE_ORDER,
                hue_order=DEVICE_ORDER, palette=custom_palette, edgecolor='black',
                linewidth=1.0, legend=False, ax=axes[1])

    for i, container in enumerate(axes[1].containers):
        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        for bar in container: bar.set_hatch(hatch)

    axes[1].set_xlabel('(b) Total Execution Cost', fontweight='bold', labelpad=15, fontsize=20)
    axes[1].set_ylabel('Total Cost (USD)', fontweight='bold', fontsize=20)
    axes[1].tick_params(axis='x', rotation=15)
    axes[1].set_yscale('log')
    axes[1].yaxis.set_major_formatter(FuncFormatter(hide_zero_formatter))
    # Add these lines before plt.tight_layout() to scale the numbers/names
    axes[0].tick_params(axis='both', which='major', labelsize=18)
    axes[1].tick_params(axis='both', which='major', labelrotation=0, labelsize=18)
    # Final Layout
    plt.tight_layout()

    plt.savefig(f"figure_combined_cost_analysis_SF_{sf}.pdf", bbox_inches='tight')
    print(f"Success: Saved figure for SF {sf}")
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_file", type=str, required=True)
    args = parser.parse_args()
    generate_combined_cost_figure(args.csv_file)
