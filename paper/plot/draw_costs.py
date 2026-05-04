#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator
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
HATCH_PATTERNS = ['/', '\\', '.', 'x', 'o', '*', '+', '-', '//']


def get_hourly_price(column_name):
    if 'pgstrom' in column_name: return None
    for device, price in AWS_HOURLY_PRICES.items():
        if device.lower() in column_name.lower(): return price
    return None


def map_device_name(column_name):
    for raw_name, clean_name in DEVICE_NAME_MAPPING.items():
        if raw_name.lower() in column_name.lower(): return clean_name
    return column_name


def style_axis(ax):
    """Applies professional, paper-ready styling to the axis spines and grid."""
    for spine in ax.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)
        spine.set_visible(True)

    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)


def generate_combined_cost_figure(csv_file):
    csv_path = Path(csv_file)
    if not csv_path.exists():
        print(f"Error: File {csv_file} does not exist.")
        return

    # 1. Load and Process Data
    df = pd.read_csv(csv_path)
    sf_match = re.search(r'SF_(\d+)', csv_path.name)
    sf = sf_match.group(1) if sf_match else "Unknown"

    df_long = pd.melt(df, id_vars=['Query'], var_name='Device', value_name='Time_Seconds').dropna(
        subset=['Time_Seconds'])
    df_long = df_long[df_long['Device'].str.contains('sedonadb', case=False, na=False)]
    df_long['Price_Per_Hour'] = df_long['Device'].apply(get_hourly_price)
    df_long = df_long.dropna(subset=['Price_Per_Hour'])

    # Calculate Cost in USD and transform query names
    df_long['Cost_USD'] = df_long['Time_Seconds'] * (df_long['Price_Per_Hour'] / 3600)
    df_long['Device'] = df_long['Device'].apply(map_device_name)
    df_long['Query'] = df_long['Query'].str.upper()

    # Natural Sorting
    df_long['Query_Num'] = df_long['Query'].str.extract(r'(\d+)').astype(int)
    df_long = df_long.sort_values(by=['Query_Num', 'Device']).drop(columns=['Query_Num'])

    print("\n--- Processed Data ---")
    print(df_long.head())

    df_total = df_long.groupby('Device')['Cost_USD'].sum().reset_index()

    print("\n--- Total Cost ---")
    print(df_total)

    # --- PLOTTING SETUP ---
    # Increased font_scale for better readability in papers
    sns.set_theme(style="whitegrid", context="paper", font_scale=2)
    plt.rcParams['font.family'] = 'sans-serif'

    custom_palette = [sns.color_palette("Set2")[i] for i in [5, 0, 1, 2]]

    fig, axes = plt.subplots(1, 2, figsize=(14, 7), gridspec_kw={'width_ratios': [1.8, 1]})

    # Custom Formatter to hide "0.0000"
    def hide_zero_formatter(x, pos):
        return f"${x:.4f}" if f"{x:.4f}" != "0.0000" else ""

    # ==========================================
    # --- PANEL A: Cost Per Query ---
    # ==========================================
    sns.barplot(data=df_long, x='Query', y='Cost_USD', hue='Device', hue_order=DEVICE_ORDER,
                palette=custom_palette, edgecolor='black', linewidth=1.2, ax=axes[0])

    # Apply Bar Hatches
    for i, container in enumerate(axes[0].containers):
        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        for bar in container:
            bar.set_hatch(hatch)

    axes[0].set_xlabel('(a) Cost per Individual Query', fontweight='bold', labelpad=15, fontsize=20)
    axes[0].set_ylabel('Individual Query Cost (USD)', fontweight='bold', fontsize=20)
    axes[0].set_yscale('log')
    axes[0].yaxis.set_major_formatter(FuncFormatter(hide_zero_formatter))

    # Ensure sufficient minor ticks on log scale
    axes[0].yaxis.set_major_locator(LogLocator(base=10.0, numticks=10))
    style_axis(axes[0])

    # Fix Legend Hatches & Styling
    leg = axes[0].legend(loc='lower left', bbox_to_anchor=(0, 0.99), ncol=4,
                         frameon=False, fontsize=18, markerscale=2, handletextpad=0.5)
    for i, patch in enumerate(leg.get_patches()):
        patch.set_hatch(HATCH_PATTERNS[i % len(HATCH_PATTERNS)])
        patch.set_edgecolor('black')
        patch.set_linewidth(1.2)

    # ==========================================
    # --- PANEL B: Total Workload Cost ---
    # ==========================================
    sns.barplot(data=df_total, x='Device', y='Cost_USD', hue='Device', order=DEVICE_ORDER,
                hue_order=DEVICE_ORDER, palette=custom_palette, edgecolor='black',
                linewidth=1.2, legend=False, ax=axes[1])

    # Apply Bar Hatches
    for i, container in enumerate(axes[1].containers):
        hatch = HATCH_PATTERNS[i % len(HATCH_PATTERNS)]
        for bar in container:
            bar.set_hatch(hatch)

    axes[1].set_xlabel('(b) Total Workload Cost', fontweight='bold', labelpad=15, fontsize=20)
    axes[1].set_ylabel('Total Cost (USD)', fontweight='bold', fontsize=20)
    axes[1].set_yscale('log')
    axes[1].yaxis.set_major_formatter(FuncFormatter(hide_zero_formatter))
    style_axis(axes[1])

    # ==========================================
    # --- FINAL LAYOUT & SIZING ---
    # ==========================================
    axes[0].tick_params(axis='both', which='major', labelsize=16)
    axes[1].tick_params(axis='both', which='major', labelrotation=0, labelsize=16)

    # Push tight_layout and then adjust top margin so legend isn't clipped
    plt.tight_layout()
    fig.subplots_adjust(top=0.88, wspace=0.25)

    output_file = f"figure_combined_cost_analysis_SF_{sf}.pdf"
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    print(f"\nSuccess: Saved high-resolution figure to {output_file}")
    plt.show()


# draw_costs.py --csv_file compiled_results_SF_10.csv
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate readable cost analysis charts.")
    parser.add_argument("--csv_file", type=str, required=True, help="Path to the input CSV file.")
    args = parser.parse_args()

    generate_combined_cost_figure(args.csv_file)
