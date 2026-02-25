#!/usr/bin/env python3
"""
Q2: Spatial Join Performance Test - Same Pattern as CSV Test
Tests GPU spatial join using external tables and views (exactly like CSV test)
"""
import sys
import time
import argparse
import sedonadb


def test_q2_with_external_tables(data_prefix=None, mode='gpu', repeat=5, target_partitions=None, zone_limit=None,
                                 trip_limit=None):
    print(f"Testing Q2 Execution with {mode.upper()} using External Tables")
    if zone_limit or trip_limit:
        print(f"Limits: zone={zone_limit if zone_limit else 'none'}, trip={trip_limit if trip_limit else 'none'}")
    else:
        print("No limits applied - using full dataset")
    print()

    # Connect and configure execution mode
    ctx = sedonadb.connect()
    if mode.lower() == 'gpu':
        ctx.sql("SET sedona.spatial_join.gpu.enable = true")
        ctx.sql("SET datafusion.execution.batch_size = 100000")
        print("GPU mode enabled")
    else:
        ctx.sql("SET sedona.spatial_join.gpu.enable = false")
        print("CPU mode enabled")

    # Configure target partitions if specified
    if target_partitions is not None:
        ctx.sql(f"SET datafusion.execution.target_partitions = {target_partitions}")
        print(f"Target partitions set to: {target_partitions}")

    print()

    # Create external tables from Parquet files (same pattern as CSV test)
    print("Creating external tables from Parquet files...")
    ctx.sql(f"""
        CREATE EXTERNAL TABLE zone_table
        STORED AS PARQUET
        LOCATION '{data_prefix}/zone/'
    """)

    ctx.sql(f"""
        CREATE EXTERNAL TABLE trip_table
        STORED AS PARQUET
        LOCATION '{data_prefix}/trip/'
    """)

    print("External tables created successfully")
    print()

    # Create views with parsed geometries (exactly like CSV test)
    print("Creating geometry views...")
    zone_limit_clause = f"LIMIT {zone_limit}" if zone_limit else ""
    trip_limit_clause = f"LIMIT {trip_limit}" if trip_limit else ""

    ctx.sql(f"""
        CREATE OR REPLACE VIEW zone_geom AS
        SELECT z_name, ST_GeomFromWKB(z_boundary) as geom
        FROM zone_table
        {zone_limit_clause}
    """)

    ctx.sql(f"""
        CREATE OR REPLACE VIEW trip_geom AS
        SELECT ST_GeomFromWKB(t_pickuploc) as geom
        FROM trip_table
        {trip_limit_clause}
    """)

    print("Views created successfully")
    print()

    # Show execution plan (should display GpuSpatialJoinExec when GPU is enabled, SpatialJoinExec for CPU)
    print(f"Execution plan for spatial join ({mode.upper()} mode):")
    plan = ctx.sql("""
        EXPLAIN
        SELECT COUNT(*)
        FROM zone_geom z
        JOIN trip_geom t
        ON ST_Intersects(z.geom, t.geom)
    --    WHERE z.z_name = 'Coconino County'
    """)
    plan.show()
    print()

    # Execute the spatial join query
    print("Running Q2 query ...")
    print("Query: Count trips in County using spatial join")
    print()
    start_time = time.time()
    for i in range(repeat + 1):
        if i == 0:
            print("Warmup Run")
        else:
            print("Run #", i)
        if i == 1:  # Start counting after warmup
            start_time = time.time()
        result = ctx.sql("""EXPLAIN ANALYZE FORMAT PGJSON
                         SELECT COUNT(*) AS trip_count_in_county
                         FROM zone_geom z
                                  JOIN trip_geom t
                                       ON ST_Intersects(z.geom, t.geom)
                         """)
        result.show()
    elapsed = (time.time() - start_time) / repeat

    print(f"Avg execution time ({mode.upper()} mode): {elapsed:.3f}s")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Q2 Spatial Join Performance Test')
    parser.add_argument('--data-prefix', '-d', type=str, default=None,
                        help='Prefix path of spatial-bench, e.g., "sf1" folder containing trip and zone (default: None)')
    parser.add_argument('mode', nargs='?', default='gpu', choices=['gpu', 'cpu'],
                        help='Execution mode: "gpu" or "cpu" (default: gpu)')
    parser.add_argument('--repeat', '-r', type=int, default=1,
                        help='Number of repeated runs (default: 5)')
    parser.add_argument('--partitions', '-p', type=int, default=None,
                        help='Number of target partitions for DataFusion (default: auto)')
    parser.add_argument('--zone-limit', type=int, default=None,
                        help='LIMIT for zone table (optional, no limit if not specified)')
    parser.add_argument('--trip-limit', type=int, default=None,
                        help='LIMIT for trip table (optional, no limit if not specified)')
    args = parser.parse_args()

    sys.exit(
        test_q2_with_external_tables(args.data_prefix, args.mode, args.repeat, args.partitions, args.zone_limit,
                                     args.trip_limit))
