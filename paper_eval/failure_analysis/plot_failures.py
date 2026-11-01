#!/usr/bin/env python3
"""
Generate publication-quality figures for the VLDB 2027 paper showcasing how
SpatialBench identifies architectural drawbacks in modern spatial engines.
"""

import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# Styling configuration for publication
plt.rcParams.update({
    'font.size': 11,
    'axes.labelsize': 12,
    'axes.titlesize': 13,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.titlesize': 14,
    'font.family': 'sans-serif',
    'figure.autolayout': True,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': '--'
})

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'plots')
os.makedirs(OUT_DIR, exist_ok=True)


def plot_duckdb_q9_optimization():
    fig, ax = plt.subplots(figsize=(6, 4))
    categories = ['Original Benchmark Query\n(PIECEWISE_MERGE_JOIN\n+ Filter Pushdown)', 
                  'Isolated Spatial Join\n(SPATIAL_JOIN via R-Tree\nOptimizer Fix)']
    latencies = [600.0, 0.213]  # in seconds (600s is timeout)
    colors = ['#d9534f', '#5cb85c']
    
    bars = ax.bar(categories, latencies, color=colors, width=0.45, edgecolor='black', linewidth=1.2)
    ax.set_yscale('log')
    ax.set_ylabel('Execution Time (seconds, log scale)')
    ax.set_title('DuckDB Q9: Spatial Self-Join Plan Degradation', fontweight='bold')
    
    # Annotations
    ax.text(0, 620, 'TIMEOUT (>600s)\n3.73B brute force checks', ha='center', va='bottom', color='#d9534f', fontweight='bold')
    ax.text(1, 0.25, '0.213s\n(>2,800x speedup)', ha='center', va='bottom', color='#3c763d', fontweight='bold')
    
    ax.set_ylim(0.05, 1500)
    plt.tight_layout()
    for ext in ['png', 'pdf']:
        fig.savefig(os.path.join(OUT_DIR, f'duckdb_q9_optimization.{ext}'), dpi=300)
    plt.close(fig)
    print("Generated duckdb_q9_optimization")


def plot_postgis_q12_knn():
    fig, ax = plt.subplots(figsize=(6.5, 4))
    
    x = np.arange(2)
    width = 0.35
    
    # 1k trips and 10k trips
    labels = ['1,000 Trips', '10,000 Trips']
    original = [53.91, 539.1]  # Extrapolated for 10k: 53.91 * 10
    knn_operator = [2.47, 10.07]
    
    rects1 = ax.bar(x - width/2, original, width, label='ST_Distance(...) LIMIT (Seq Scan)', color='#f0ad4e', edgecolor='black')
    rects2 = ax.bar(x + width/2, knn_operator, width, label='<-> Operator (GiST KNN Index Scan)', color='#0275d8', edgecolor='black')
    
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('PostGIS Q12: Standard Order By vs. GiST KNN Operator (<->)', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    
    # Annotations
    ax.text(x[0] - width/2, 53.91 + 5, '53.9s', ha='center', va='bottom', fontsize=9)
    ax.text(x[0] + width/2, 2.47 + 5, '2.47s (22x)', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax.text(x[1] - width/2, 539.1 + 5, '539.1s*', ha='center', va='bottom', fontsize=9)
    ax.text(x[1] + width/2, 10.07 + 5, '10.1s (53x)', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax.set_ylim(0, 650)
    plt.tight_layout()
    for ext in ['png', 'pdf']:
        fig.savefig(os.path.join(OUT_DIR, f'postgis_q12_knn.{ext}'), dpi=300)
    plt.close(fig)
    print("Generated postgis_q12_knn")


def plot_duckdb_q10_vertex_complexity():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8.5, 4))
    
    # Subplot 1: Zone Counts
    zone_types = ['Non-County Zones\n(Avg 45 vertices)', 'County Zones\n(Avg 2,028 vertices\nMax 345,461 vertices)']
    counts = [416.7, 38.0]  # in thousands
    bars1 = ax1.bar(zone_types, counts, color=['#5bc0de', '#d9534f'], width=0.45, edgecolor='black')
    ax1.set_ylabel('Number of Zones (Thousands)')
    ax1.set_title('(a) Zone Partition Count', fontweight='bold')
    for bar, val in zip(bars1, counts):
        ax1.text(bar.get_x() + bar.get_width()/2, val + 10, f'{val:,.1f}k', ha='center', va='bottom')
    ax1.set_ylim(0, 480)
    
    # Subplot 2: Execution Time on 1M Trips
    times = [0.597, 17.67]  # seconds
    bars2 = ax2.bar(zone_types, times, color=['#5bc0de', '#d9534f'], width=0.45, edgecolor='black')
    ax2.set_ylabel('Point-in-Polygon Time (seconds)')
    ax2.set_title('(b) Runtime on 1M Trips', fontweight='bold')
    for bar, val in zip(bars2, times):
        ax2.text(bar.get_x() + bar.get_width()/2, val + 0.5, f'{val:.2f}s', ha='center', va='bottom', fontweight='bold')
    ax2.text(1, 19, '30x Slower Refinement!', ha='center', va='bottom', color='#d9534f', fontweight='bold')
    ax2.set_ylim(0, 22)
    
    plt.suptitle('DuckDB Q10: Ray-Casting Refinement Cost on Complex Polygons', fontweight='bold')
    plt.tight_layout()
    for ext in ['png', 'pdf']:
        fig.savefig(os.path.join(OUT_DIR, f'duckdb_q10_vertex_complexity.{ext}'), dpi=300)
    plt.close(fig)
    print("Generated duckdb_q10_vertex_complexity")


def plot_engine_summary_matrix():
    # Heatmap of query outcomes across engines at SF10
    fig, ax = plt.subplots(figsize=(10, 4.5))
    
    engines = ['SedonaDB', 'DuckDB', 'PostGIS (NoIdx)', 'PostGIS (Idx)', 'ClickHouse', 'GeoPandas']
    queries = [f'Q{i}' for i in range(1, 13)]
    
    # Status codes: 0 = Success, 1 = Timeout (>600s), 2 = OOM / Kill, 3 = Error / Bug
    # Data from benchmark runs at SF10
    status_matrix = np.array([
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],       # SedonaDB: all pass
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2],       # DuckDB: Q9, Q10, Q11 timeout, Q12 OOM
        [0, 0, 0, 1, 0, 1, 0, 1, 3, 1, 1, 1],       # PostGIS NoIdx: Q4,6,8,10,11,12 TO, Q9 GEOS error
        [0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 1, 1],       # PostGIS Idx: Q9 GEOS error, Q10,11,12 TO
        [0, 0, 0, 2, 0, 1, 0, 1, 1, 2, 1, 3],       # ClickHouse: Q4,10 OOM, Q6,8,9,11 TO, Q12 bug
        [0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 2, 2],       # GeoPandas: Q8,10,11,12 OOM
    ])
    
    # Custom colormap
    from matplotlib.colors import ListedColormap
    # 0: Green (Success), 1: Orange (Timeout), 2: Red (OOM), 3: Purple (Error)
    cmap = ListedColormap(['#a1d99b', '#fdd0a2', '#fc9272', '#bcbddc'])
    
    im = ax.imshow(status_matrix, cmap=cmap, aspect='auto')
    
    ax.set_xticks(np.arange(len(queries)))
    ax.set_yticks(np.arange(len(engines)))
    ax.set_xticklabels(queries)
    ax.set_yticklabels(engines, fontweight='bold')
    
    # Text labels in cells
    labels_map = {0: 'PASS', 1: 'TIMEOUT', 2: 'OOM', 3: 'ERROR'}
    for i in range(len(engines)):
        for j in range(len(queries)):
            code = status_matrix[i, j]
            txt = labels_map[code]
            color = 'black' if code == 0 else ('#800000' if code in [1, 2] else '#4a148c')
            ax.text(j, i, txt, ha='center', va='center', fontsize=8, color=color, fontweight='bold' if code != 0 else 'normal')
            
    # Add legend
    import matplotlib.patches as mpatches
    legend_elements = [
        mpatches.Patch(facecolor='#a1d99b', edgecolor='black', label='Pass (<600s)'),
        mpatches.Patch(facecolor='#fdd0a2', edgecolor='black', label='Timeout (>600s)'),
        mpatches.Patch(facecolor='#fc9272', edgecolor='black', label='OOM / SIGKILL'),
        mpatches.Patch(facecolor='#bcbddc', edgecolor='black', label='Fatal Syntax / Overlay Error')
    ]
    ax.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=4)
    
    ax.set_title('SpatialBench SF10 Benchmark Status Matrix Across Engines', fontweight='bold', pad=12)
    plt.tight_layout()
    for ext in ['png', 'pdf']:
        fig.savefig(os.path.join(OUT_DIR, f'spatialbench_summary_matrix.{ext}'), dpi=300)
    plt.close(fig)
    print("Generated spatialbench_summary_matrix")


if __name__ == '__main__':
    plot_duckdb_q9_optimization()
    plot_postgis_q12_knn()
    plot_duckdb_q10_vertex_complexity()
    plot_engine_summary_matrix()
    print("All plots generated successfully in:", OUT_DIR)
