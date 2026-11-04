#!/usr/bin/env python3
"""
Thrust 7: Algorithmic Polygon Complexity Stress Test.
Evaluates query execution scaling as polygon vertex complexity increases:
V in {4, 16, 64, 256, 1024, 4096, 16384, 65536}
against 1,000,000 trip points.
Compares:
1. Unindexed Scalar Ray-Casting (DuckDB ST_Within / ST_Intersects)
2. Edge-Indexed Point-in-Polygon (GEOS / Shapely PreparedGeometry with hierarchical interval tree)
"""

import json
import math
import os
import time
from pathlib import Path
import duckdb
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, Point
from shapely.prepared import prep
import shapely.wkt

RESULTS_DIR = Path("paper_eval/results")
PLOTS_DIR = Path("paper_eval/plots")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def generate_complex_polygon(num_vertices: int, cx=-112.0, cy=34.5, r=0.15) -> str:
    """Generate a valid non-self-intersecting complex polygon with specified vertex count."""
    angles = np.linspace(0, 2 * np.pi, num_vertices, endpoint=False)
    # Add gentle radial harmonic perturbation to simulate intricate coastal/administrative boundaries
    radii = r * (1.0 + 0.15 * np.sin(5 * angles) + 0.08 * np.cos(11 * angles))
    xs = cx + radii * np.cos(angles)
    ys = cy + radii * np.sin(angles)
    coords = list(zip(xs, ys))
    coords.append(coords[0]) # close ring
    poly = Polygon(coords)
    return poly.wkt

def benchmark_polygon_complexity():
    vertex_counts = [4, 16, 64, 256, 1024, 4096, 16384, 65536]
    N_points = 500_000
    
    print(f"=== Thrust 7: Algorithmic Polygon Complexity Stress Test (N={N_points:,} points) ===")
    
    # 1. Initialize DuckDB with spatial extension and sample points
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET threads = 8;")
    con.execute(f"""
    CREATE TABLE test_points AS 
    SELECT t_tripkey, ST_Point(ST_X(ST_GeomFromWKB(t_pickuploc)), ST_Y(ST_GeomFromWKB(t_pickuploc))) as pt,
           ST_X(ST_GeomFromWKB(t_pickuploc)) as x, ST_Y(ST_GeomFromWKB(t_pickuploc)) as y
    FROM "paper_eval/benchmark-data-sf1/trip/*.parquet"
    LIMIT {N_points};
    """)
    print("DuckDB loaded points table.")

    # Extract numpy coordinates for Shapely PreparedGeometry evaluation
    pts_df = con.execute("SELECT x, y FROM test_points").df()
    pts = [Point(xy) for xy in zip(pts_df["x"].values, pts_df["y"].values)]

    results = {
        "vertex_counts": vertex_counts,
        "unindexed_raycast_ms": [],
        "edge_indexed_prepared_ms": [],
    }

    for V in vertex_counts:
        wkt = generate_complex_polygon(V)
        poly_geom = shapely.wkt.loads(wkt)
        
        # 1. Edge-Indexed PreparedGeometry (GEOS / PostGIS / Shapely)
        t_prep_start = time.perf_counter()
        prepared_poly = prep(poly_geom)
        prep_time = time.perf_counter() - t_prep_start
        
        t0 = time.perf_counter()
        # Evaluate containment using edge index
        _ = sum(1 for pt in pts if prepared_poly.contains(pt))
        prepared_latency = time.perf_counter() - t0
        
        # 2. DuckDB Unindexed Ray-Casting
        con.execute(f"SET VARIABLE target_poly = ST_GeomFromText('{wkt}');")
        
        t0 = time.perf_counter()
        con.execute("SELECT count(*) FROM test_points WHERE ST_Within(pt, getvariable('target_poly'));").fetchall()
        duck_latency = time.perf_counter() - t0

        print(f"V={V:5d} vertices | DuckDB Ray-Cast: {duck_latency*1000:7.1f} ms | PreparedGeometry: {prepared_latency*1000:7.1f} ms (Prep: {prep_time*1000:.2f}ms)")
        results["unindexed_raycast_ms"].append(round(duck_latency * 1000, 2))
        results["edge_indexed_prepared_ms"].append(round(prepared_latency * 1000, 2))

    with open(RESULTS_DIR / "polygon_complexity_results.json", "w") as f:
        json.dump(results, f, indent=2)

    plot_complexity(results)

def plot_complexity(results):
    V = results["vertex_counts"]
    raycast = results["unindexed_raycast_ms"]
    prepared = results["edge_indexed_prepared_ms"]

    fig, ax = plt.subplots(figsize=(9, 6), dpi=300)

    ax.plot(V, raycast, "o-", color="#e41a1c", linewidth=2.5, markersize=8, label="Unindexed Scalar Ray-Casting (O(V))")
    ax.plot(V, prepared, "s--", color="#377eb8", linewidth=2.5, markersize=8, label="Hierarchical Edge-Indexed / Prepared (O(log V))")

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Polygon Vertex Count (V, Log Scale)", fontsize=11, fontweight="bold")
    ax.set_ylabel("Containment Query Latency (ms, Log Scale)", fontsize=11, fontweight="bold")
    ax.set_title("Point-in-Polygon Scalability vs Vertex Complexity (Thrust 7)", fontsize=12, fontweight="bold")
    ax.grid(True, which="both", linestyle=":", alpha=0.5)
    ax.legend(fontsize=10, loc="upper left")

    # Annotate gap at 65536 vertices
    speedup = raycast[-1] / max(1e-6, prepared[-1])
    ax.annotate(f"{speedup:.1f}x Advantage\nvia Edge Indexing",
                xy=(V[-1], raycast[-1]), xytext=(V[-2], raycast[-1] * 0.4),
                arrowprops=dict(facecolor="black", shrink=0.08, width=1, headwidth=6),
                fontweight="bold", fontsize=10, ha="center")

    plt.tight_layout()
    png_path = PLOTS_DIR / "polygon_complexity.png"
    pdf_path = PLOTS_DIR / "polygon_complexity.pdf"
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
    print(f"Saved Polygon Complexity plots to {png_path} and {pdf_path}")

if __name__ == "__main__":
    benchmark_polygon_complexity()
