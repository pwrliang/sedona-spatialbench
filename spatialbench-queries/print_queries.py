#!/usr/bin/env python3
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import inspect
import re
import sys


class SpatialBenchBenchmark:
    """A benchmark for the performance of analytical spatial queries on a spatial dataset.

    These queries are written in the Sedona/Spark SQL dialect. Because spatial functions are not as standardized as
    other analytical functions, many engines needs specific implementations of a couple of these queries where dialects
    vary slightly.

    To deal with these differences,  other engine-specific implementations of this benchmark subclass this class and
    override only the queries that need to be changed.

    """

    def queries(self) -> dict[str, str]:
        """
        Collects all methods of the subclass whose names start with 'q' followed by a number and have no arguments (other than self),
        and returns them as a dictionary of query functions, partially applied with the current instance.

        Returns:
            Dict[str, str]: A dictionary mapping query names to their corresponding functions.
        """

        queries = {}
        for name, method in inspect.getmembers(
                self.__class__, predicate=inspect.isfunction
        ):
            if re.fullmatch(r"q\d+", name):
                sig = inspect.signature(method)
                if len(sig.parameters) == 0:
                    queries[name] = method()
                else:
                    raise ValueError("Query methods must not take any arguments")

        # Sort queries numerically by extracting the number from the query name
        sorted_queries = dict(sorted(queries.items(), key=lambda x: int(x[0][1:])))

        return sorted_queries

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "SedonaSpark"

    @staticmethod
    def q1() -> str:
        return """
               -- Q1: Find trips starting within 50km of Sedona city center, ordered by distance
               SELECT t.t_tripkey,
                      ST_X(ST_GeomFromWKB(t.t_pickuploc))                       AS pickup_lon,
                      ST_Y(ST_GeomFromWKB(t.t_pickuploc))                       AS pickup_lat,
                      t.t_pickuptime,
                      ST_Distance(ST_GeomFromWKB(t.t_pickuploc),
                                  ST_GeomFromText('POINT (-111.7610 34.8697)')) AS distance_to_center
               FROM trip t
               WHERE ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)'),
                                0.45) -- 50km radius around Sedona center
               ORDER BY distance_to_center ASC, t.t_tripkey ASC
               """

    @staticmethod
    def q2() -> str:
        return """
               -- Q2: Count trips starting within Coconino County (Arizona) zone
               SELECT COUNT(*) AS trip_count_in_coconino_county
               FROM trip t
               WHERE ST_Intersects(ST_GeomFromWKB(t.t_pickuploc), (SELECT ST_GeomFromWKB(z.z_boundary)
                                                                   FROM zone z
                                                                   WHERE z.z_name = 'Coconino County' LIMIT 1))
               """

    @staticmethod
    def q3() -> str:
        return """
               -- Q3: Monthly trip statistics within 15km radius of Sedona city center (10km base + 5km buffer)
               SELECT DATE_TRUNC('month', t.t_pickuptime)   AS pickup_month,
                      COUNT(t.t_tripkey)                    AS total_trips,
                      AVG(t.t_distance)                     AS avg_distance,
                      AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
                      AVG(t.t_fare)                         AS avg_fare
               FROM trip t
               WHERE ST_DWithin(
                             ST_GeomFromWKB(t.t_pickuploc),
                             ST_GeomFromText('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'), -- 10km bounding box around Sedona
                             0.045 -- Additional 5km buffer
                     )
               GROUP BY pickup_month
               ORDER BY pickup_month \
               """

    @staticmethod
    def q4() -> str:
        return """
               -- Q4: Zone distribution of top 1000 trips by tip amount
               SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
               FROM zone z
                        JOIN (SELECT t.t_pickuploc
                              FROM trip t
                              ORDER BY t.t_tip DESC, t.t_tripkey
                                               ASC LIMIT 1000 -- Replace 1000 with x (how many top tips you want)
               ) top_trips ON ST_Within(ST_GeomFromWKB(top_trips.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
               GROUP BY z.z_zonekey, z.z_name
               ORDER BY trip_count DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q5() -> str:
        return """
               -- Q5: Monthly travel patterns for repeat customers (convex hull of dropoff locations)
               SELECT c.c_custkey,
                      c.c_name                                                                      AS customer_name,
                      DATE_TRUNC('month', t.t_pickuptime)                                           AS pickup_month,
                      ST_Area(ST_ConvexHull(ST_Collect(ARRAY_AGG(ST_GeomFromWKB(t.t_dropoffloc))))) AS monthly_travel_hull_area,
                      COUNT(*)                                                                      as dropoff_count
               FROM trip t
                        JOIN customer c ON t.t_custkey = c.c_custkey
               GROUP BY c.c_custkey, c.c_name, pickup_month
               HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
               ORDER BY dropoff_count DESC, c.c_custkey ASC \
               """

    @staticmethod
    def q6() -> str:
        return """
               -- Q6: Zone statistics for trips intersecting a bounding box
               SELECT z.z_zonekey,
                      z.z_name,
                      COUNT(t.t_tripkey)                    AS total_pickups,
                      AVG(t.t_totalamount)                  AS avg_distance,
                      AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
               FROM trip t,
                    zone z
               WHERE ST_Intersects(
                       ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'),
                       ST_GeomFromWKB(z.z_boundary))
                 AND ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
               GROUP BY z.z_zonekey, z.z_name
               ORDER BY total_pickups DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q7() -> str:
        return """
               -- Q7: Detect potential route detours by comparing reported vs. geometric distances
               WITH trip_lengths AS (SELECT t.t_tripkey,
                                            t.t_distance AS reported_distance_m,
                                            ST_Length(
                                                    ST_MakeLine(
                                                            ST_GeomFromWKB(t.t_pickuploc),
                                                            ST_GeomFromWKB(t.t_dropoffloc)
                                                    )
                                            ) / 0.000009 AS line_distance_m -- 1 meter = 0.000009 degree
                                     FROM trip t)
               SELECT t.t_tripkey,
                      t.reported_distance_m,
                      t.line_distance_m,
                      t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
               FROM trip_lengths t
               ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
               """

    @staticmethod
    def q8() -> str:
        return """
               -- Q8: Count nearby pickups for each building within 500m radius
               SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
               FROM trip t
                        JOIN building b
                             ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045) -- ~500m
               GROUP BY b.b_buildingkey, b.b_name
               ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
               """

    @staticmethod
    def q9() -> str:
        return """
               -- Q9: Building Conflation (duplicate/overlap detection via IoU), deterministic order
               WITH b1 AS (SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom
                           FROM building),
                    b2 AS (SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom
                           FROM building),
                    pairs AS (SELECT b1.id                                      AS building_1,
                                     b2.id                                      AS building_2,
                                     ST_Area(b1.geom)                           AS area1,
                                     ST_Area(b2.geom)                           AS area2,
                                     ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
                              FROM b1
                                       JOIN b2
                                            ON b1.id < b2.id
                                                AND ST_Intersects(b1.geom, b2.geom))
               SELECT building_1,
                      building_2,
                      area1,
                      area2,
                      overlap_area,
                      CASE
                          WHEN overlap_area = 0 THEN 0.0
                          WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
                          ELSE overlap_area / (area1 + area2 - overlap_area)
                          END AS iou
               FROM pairs
               ORDER BY iou DESC, building_1 ASC, building_2 ASC
               """

    @staticmethod
    def q10() -> str:
        return """
               -- Q10: Zone statistics for trips starting within each zone
               SELECT z.z_zonekey,
                      z.z_name                              AS pickup_zone,
                      AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
                      AVG(t.t_distance)                     AS avg_distance,
                      COUNT(t.t_tripkey)                    AS num_trips
               FROM zone z
                        LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
               GROUP BY z.z_zonekey, z.z_name
               ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
               """

    @staticmethod
    def q11() -> str:
        return """
               -- Q11: Count trips that cross between different zones
               SELECT COUNT(*) AS cross_zone_trip_count
               FROM trip t
                        JOIN zone pickup_zone
                             ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
                        JOIN zone dropoff_zone
                             ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
               WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
               """

    @staticmethod
    def q12() -> str:
        # There is some odd bug with missing columns in EMR. Using CTEs to work around it.
        return """
               -- Q12: Find 5 nearest buildings to each trip pickup location using KNN join
               WITH trip_with_geom AS (SELECT t_tripkey, t_pickuploc, ST_GeomFromWKB(t_pickuploc) as pickup_geom
                                       FROM trip),
                    building_with_geom AS (SELECT b_buildingkey,
                                                  b_name,
                                                  b_boundary,
                                                  ST_GeomFromWKB(b_boundary) as boundary_geom
                                           FROM building)
               SELECT t.t_tripkey,
                      t.t_pickuploc,
                      b.b_buildingkey,
                      b.b_name                                    AS building_name,
                      ST_Distance(t.pickup_geom, b.boundary_geom) AS distance_to_building
               FROM trip_with_geom t
                        JOIN building_with_geom b
                             ON ST_KNN(t.pickup_geom, b.boundary_geom, 5, FALSE)
               ORDER BY distance_to_building ASC, b.b_buildingkey ASC
               """


class DatabricksSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A Databricks-specific implementation of the SpatialBench benchmark.

    This class is used to run the SpatialBench benchmark using Databricks' spatial functions. It varies only as
    needed from the base class.

    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "Databricks"

    @staticmethod
    def q5() -> str:
        return """
               -- Q5 (Databricks): NO ST_Collect function, using ST_Union_Agg instead. This is more expensive, but should give the same results.
               SELECT c.c_custkey,
                      c.c_name                                                             AS customer_name,
                      DATE_TRUNC('month', t.t_pickuptime)                                  AS pickup_month,
                      ST_Area(ST_ConvexHull(ST_Union_Agg(ST_GeomFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area,
                      COUNT(*)                                                             as dropoff_count
               FROM trip t
                        JOIN customer c ON t.t_custkey = c.c_custkey
               GROUP BY c.c_custkey, c.c_name, pickup_month
               HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
               ORDER BY dropoff_count DESC, c.c_custkey ASC
               """

    @staticmethod
    def q7() -> str:
        return """
               -- Q7 (Databricks): ST_MakeLine takes an array of points rather than varargs
               WITH trip_lengths AS (SELECT t.t_tripkey,
                                            t.t_distance AS reported_distance_m,
                                            ST_Length(
                                                    ST_MakeLine(
                                                            Array(
                                                                    ST_GeomFromWKB(t.t_pickuploc),
                                                                    ST_GeomFromWKB(t.t_dropoffloc)
                                                            )
                                                    )
                                            ) / 0.000009 AS line_distance_m -- 1 meter = 0.000009 degree
                                     FROM trip t)
               SELECT t.t_tripkey,
                      t.reported_distance_m,
                      t.line_distance_m,
                      t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
               FROM trip_lengths t
               ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
               """

    @staticmethod
    def q12() -> str:
        return """
               -- Q12 (Databricks): No KNN join, using cross join + ROW_NUMBER() window function instead.
-- Note: Databricks doesn't have  cross join lateral support.
               SELECT t_tripkey,
                      t_pickuploc,
                      b_buildingkey,
                      building_name,
                      distance_to_building
               FROM (SELECT t.t_tripkey,
                            t.t_pickuploc,
                            b.b_buildingkey,
                            b.b_name                                  AS building_name,
                            ST_Distance(ST_GeomFromWKB(t.t_pickuploc),
                                        ST_GeomFromWKB(b.b_boundary)) AS distance_to_building,
                            ROW_NUMBER()                                 OVER (
        PARTITION BY t.t_tripkey
        ORDER BY ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) ASC
    ) AS rn
                     FROM trip t
                              JOIN building b) AS ranked_buildings
               WHERE rn <= 5
               ORDER BY distance_to_building ASC, b_buildingkey ASC
               """


class DuckDBSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A DuckDB-specific implementation of the SpatialBench benchmark.

    This class is used to run the SpatialBench benchmark using DuckDB's spatial extension. It varies only as
    needed from the base class.
    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "DuckDB"

    @staticmethod
    def q12() -> str:
        return """
               -- Q12 (DuckDB): No KNN join, using cross join lateral instead.
               SELECT t.t_tripkey,
                      t.t_pickuploc,
                      nb.b_buildingkey,
                      nb.building_name,
                      nb.distance_to_building
               FROM trip t
                        CROSS JOIN LATERAL (
                   SELECT b.b_buildingkey,
                          b.b_name                                  AS building_name,
                          ST_Distance(ST_GeomFromWKB(t.t_pickuploc),
                                      ST_GeomFromWKB(b.b_boundary)) AS distance_to_building
                   FROM building b
                   ORDER BY distance_to_building
                       LIMIT 5
) AS nb
               ORDER BY nb.distance_to_building, nb.b_buildingkey
               """


class SedonaDBSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A SedonaDB-specific implementation of the SpatialBench benchmark.

    This class is used to run the SpatialBench benchmark using SedonaDB's spatial functions.
    It inherits from the SpatialBenchBenchmark class and uses SedonaDB's spatial functions.

    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "SedonaDB"

    @staticmethod
    def q5() -> str:
        return """
               -- Q5 (SedonaDB): SedonaDB uses ST_Collect_Agg (with _Agg suffix) for aggregate functions.
               SELECT c.c_custkey,
                      c.c_name                                                               AS customer_name,
                      DATE_TRUNC('month', t.t_pickuptime)                                    AS pickup_month,
                      ST_Area(ST_ConvexHull(ST_Collect_Agg(ST_GeomFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area,
                      COUNT(*)                                                               as dropoff_count
               FROM trip t
                        JOIN customer c ON t.t_custkey = c.c_custkey
               GROUP BY c.c_custkey, c.c_name, pickup_month
               HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
               ORDER BY dropoff_count DESC, c.c_custkey ASC
               """


class PgStromSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A PG-Strom-specific implementation of the SpatialBench benchmark.

    PG-Strom uses PostGIS syntax but performs best when ST_GeomFromWKB is removed
    to allow the GPU to access native geometry types directly.
    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "PgStrom"

    @staticmethod
    def q1() -> str:
        return """
               -- Q1: Find trips starting within 50km of Sedona city center
               SELECT t.t_tripkey,
                      ST_X(t.t_pickuploc)                                             AS pickup_lon,
                      ST_Y(t.t_pickuploc)                                             AS pickup_lat,
                      t.t_pickuptime,
                      ST_Distance(t.t_pickuploc,
                                  ST_GeomFromText('POINT (-111.7610 34.8697)', 4326)) AS distance_to_center
               FROM trip t
               WHERE ST_DWithin(t.t_pickuploc, ST_GeomFromText('POINT (-111.7610 34.8697)', 4326), 0.45)
               ORDER BY distance_to_center ASC, t.t_tripkey ASC
               """

    @staticmethod
    def q2() -> str:
# ==================== QUERY PLAN ====================
# Finalize Aggregate  (cost=69358.27..69358.28 rows=1 width=8)
#   Output: count(*)
#   InitPlan 1 (returns $1)
#     ->  Limit  (cost=1100.00..14218.94 rows=1 width=4521)
#           Output: z.z_boundary
#           ->  Gather  (cost=1100.00..40456.81 rows=3 width=4521)
#                 Output: z.z_boundary
#                 Workers Planned: 2
# -->                 ->  Parallel Custom Scan (GpuScan) on public.zone z  (cost=100.00..39456.51 rows=1 width=4521)
#                       Output: z.z_boundary
#                       GPU Projection: z.z_boundary
#                       GPU Scan Quals: (z.z_name = 'Coconino County'::text) [plan: 454710 -> 1]
#                       Scan-Engine: VFS with GPU0
#   ->  Gather  (cost=55139.11..55139.32 rows=2 width=8)
#         Output: (PARTIAL count(*))
#         Workers Planned: 2
#         Params Evaluated: $1
#         ->  Partial Aggregate  (cost=54139.11..54139.12 rows=1 width=8)
#               Output: PARTIAL count(*)
#               ->  Parallel Bitmap Heap Scan on public.trip t  (cost=170.92..54132.86 rows=2500 width=0)
#                     Filter: st_intersects(t.t_pickuploc, $1)
#                     ->  Bitmap Index Scan on idx_trip_t_pickuploc  (cost=0.00..169.42 rows=6000 width=0)
#                           Index Cond: (t.t_pickuploc && $1)
# ======================================================
        return """
               -- Q2: Count trips starting within Coconino County (Arizona) zone
               SELECT COUNT(*) AS trip_count_in_coconino_county
               FROM trip t
               WHERE ST_Intersects(t.t_pickuploc,
                                   (SELECT z.z_boundary FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1))
               """

    @staticmethod
    def q3() -> str:
        return """
               -- Q3: Monthly trip statistics within 15km radius of Sedona city center
               SELECT DATE_TRUNC('month', t.t_pickuptime)   AS pickup_month,
                      COUNT(t.t_tripkey)                    AS total_trips,
                      AVG(t.t_distance)                     AS avg_distance,
                      AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
                      AVG(t.t_fare)                         AS avg_fare
               FROM trip t
               WHERE ST_DWithin(
                             t.t_pickuploc,
                             ST_GeomFromText(
                                     'POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))',
                                     4326),
                             0.045
                     )
               GROUP BY pickup_month
               ORDER BY pickup_month \
               """

    @staticmethod
    def q4() -> str:
# ==================== QUERY PLAN ====================
# Sort  (cost=3606870.09..3608006.87 rows=454710 width=31)
#   Output: z.z_zonekey, z.z_name, (count(*))
#   Sort Key: (count(*)) DESC, z.z_zonekey
#   ->  HashAggregate  (cost=3559592.56..3564139.66 rows=454710 width=31)
#         Output: z.z_zonekey, z.z_name, count(*)
#         Group Key: z.z_zonekey, z.z_name
# -->         ->  Nested Loop  (cost=2843361.34..3556182.23 rows=454710 width=23)
#               Output: z.z_zonekey, z.z_name
#               ->  Limit  (cost=2843361.06..2843477.73 rows=1000 width=45)
#                     Output: t.t_pickuploc, t.t_tip, t.t_tripkey
#                     ->  Gather Merge  (cost=2843361.06..8677101.68 rows=50000000 width=45)
#                           Output: t.t_pickuploc, t.t_tip, t.t_tripkey
#                           Workers Planned: 2
#                           ->  Sort  (cost=2842361.04..2904861.04 rows=25000000 width=45)
#                                 Output: t.t_pickuploc, t.t_tip, t.t_tripkey
#                                 Sort Key: t.t_tip DESC, t.t_tripkey
#                                 ->  Parallel Seq Scan on public.trip t  (cost=0.00..1471638.00 rows=25000000 width=45)
#                                       Output: t.t_pickuploc, t.t_tip, t.t_tripkey
#               ->  Index Scan using idx_zone_z_boundary on public.zone z  (cost=0.29..712.24 rows=45 width=4544)
#                     Output: z.z_zonekey, z.z_gersid, z.z_country, z.z_region, z.z_name, z.z_subtype, z.z_boundary
#                     Index Cond: (z.z_boundary ~ t.t_pickuploc)
#                     Filter: st_within(t.t_pickuploc, z.z_boundary)
# ======================================================
        return """
               -- Q4: Zone distribution of top 1000 trips by tip amount
               SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
               FROM zone z
                        JOIN (SELECT t.t_pickuploc
                              FROM trip t
                              ORDER BY t.t_tip DESC, t.t_tripkey ASC LIMIT 1000) top_trips
                             ON ST_Within(top_trips.t_pickuploc, z.z_boundary)
               GROUP BY z.z_zonekey, z.z_name
               ORDER BY trip_count DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q5() -> str:
        return """
               -- Q5: Monthly travel patterns for repeat customers
               SELECT c.c_custkey,
                      c.c_name                                           AS customer_name,
                      DATE_TRUNC('month', t.t_pickuptime)                AS pickup_month,
                      ST_Area(ST_ConvexHull(ST_Collect(t.t_dropoffloc))) AS monthly_travel_hull_area,
                      COUNT(*)                                           as dropoff_count
               FROM trip t
                        JOIN customer c ON t.t_custkey = c.c_custkey
               GROUP BY c.c_custkey, c.c_name, pickup_month
               HAVING COUNT(*) > 5
               ORDER BY dropoff_count DESC, c.c_custkey ASC \
               """

    @staticmethod
    def q6() -> str:
# ==================== QUERY PLAN ====================
# Sort  (cost=98737.63..98737.63 rows=1 width=79)
#   Output: z.z_zonekey, z.z_name, (count(t.t_tripkey)), (avg(t.t_totalamount)), (avg((t.t_dropofftime - t.t_pickuptime)))
#   Sort Key: (count(t.t_tripkey)) DESC, z.z_zonekey
#   ->  GroupAggregate  (cost=98581.98..98737.62 rows=1 width=79)
#         Output: z.z_zonekey, z.z_name, count(t.t_tripkey), avg(t.t_totalamount), avg((t.t_dropofftime - t.t_pickuptime))
#         Group Key: z.z_zonekey, z.z_name
#         ->  Sort  (cost=98581.98..98604.21 rows=8893 width=54)
#               Output: z.z_zonekey, z.z_name, t.t_tripkey, t.t_totalamount, t.t_dropofftime, t.t_pickuptime
#               Sort Key: z.z_zonekey, z.z_name
# -->               ->  Nested Loop  (cost=171.20..97998.67 rows=8893 width=54)
#                     Output: z.z_zonekey, z.z_name, t.t_tripkey, t.t_totalamount, t.t_dropofftime, t.t_pickuptime
#                     ->  Index Scan using idx_zone_z_boundary on public.zone z  (cost=0.29..20.80 rows=1 width=4544)
#                           Output: z.z_zonekey, z.z_gersid, z.z_country, z.z_region, z.z_name, z.z_subtype, z.z_boundary
#                           Index Cond: (z.z_boundary && '0103000020E610000001000000050000002FDD2406810D5CC0CB10C7BAB835414096438B6CE7D35BC0CB10C7BAB835414096438B6CE7D35BC0FE43FAEDEBA841402FDD2406810D5CC0FE43FAEDEBA841402FDD2406810D5CC0CB10C7BAB8354140'::geometry)
#                           Filter: st_intersects('0103000020E610000001000000050000002FDD2406810D5CC0CB10C7BAB835414096438B6CE7D35BC0CB10C7BAB835414096438B6CE7D35BC0FE43FAEDEBA841402FDD2406810D5CC0FE43FAEDEBA841402FDD2406810D5CC0CB10C7BAB8354140'::geometry, z.z_boundary)
#                     ->  Bitmap Heap Scan on public.trip t  (cost=170.92..97917.86 rows=6000 width=63)
#                           Output: t.t_tripkey, t.t_custkey, t.t_driverkey, t.t_vehiclekey, t.t_pickuptime, t.t_dropofftime, t.t_fare, t.t_tip, t.t_totalamount, t.t_distance, t.t_pickuploc, t.t_dropoffloc
#                           Filter: st_within(t.t_pickuploc, z.z_boundary)
#                           ->  Bitmap Index Scan on idx_trip_t_pickuploc  (cost=0.00..169.42 rows=6000 width=0)
#                                 Index Cond: (t.t_pickuploc @ z.z_boundary)
# ======================================================
        return """
               -- Q6: Zone statistics for trips intersecting a bounding box
               SELECT z.z_zonekey,
                      z.z_name,
                      COUNT(t.t_tripkey)                    AS total_pickups,
                      AVG(t.t_totalamount)                  AS avg_distance,
                      AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
               FROM trip t,
                    zone z
               WHERE ST_Intersects(ST_GeomFromText(
                                           'POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))',
                                           4326), z.z_boundary)
                 AND ST_Within(t.t_pickuploc, z.z_boundary)
               GROUP BY z.z_zonekey, z.z_name
               ORDER BY total_pickups DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q7() -> str:
        return """
               -- Q7: Detect potential route detours
               WITH trip_lengths AS (SELECT t.t_tripkey,
                                            t.t_distance                                                     AS reported_distance_m,
                                            ST_Length(ST_MakeLine(t.t_pickuploc, t.t_dropoffloc)) / 0.000009 AS line_distance_m
                                     FROM trip t)
               SELECT t.t_tripkey,
                      t.reported_distance_m,
                      t.line_distance_m,
                      t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
               FROM trip_lengths t
               ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
               """

    @staticmethod
    def q8() -> str:
        return """
               -- Q8: Count nearby pickups for each building within 500m radius
               SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
               FROM trip t
                        JOIN building b ON ST_DWithin(t.t_pickuploc, b.b_boundary, 0.0045)
               GROUP BY b.b_buildingkey, b.b_name
               ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
               """

    @staticmethod
    def q9() -> str:

        # ==================== QUERY PLAN ====================
        # Gather Merge  (cost=30876835.30..30921987.98 rows=392632 width=48)
        #   Output: b1.b_buildingkey, b2.b_buildingkey, (st_area(b1.b_boundary)), (st_area(b2.b_boundary)), (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision))), (CASE WHEN (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) = '0'::double precision) THEN '0'::double precision WHEN (((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision))) = '0'::double precision) THEN '1'::double precision ELSE (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) / ((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)))) END)
        #   Workers Planned: 1
        #   ->  Sort  (cost=30875835.29..30876816.87 rows=392632 width=48)
        #         Output: b1.b_buildingkey, b2.b_buildingkey, (st_area(b1.b_boundary)), (st_area(b2.b_boundary)), (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision))), (CASE WHEN (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) = '0'::double precision) THEN '0'::double precision WHEN (((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision))) = '0'::double precision) THEN '1'::double precision ELSE (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) / ((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)))) END)
        #         Sort Key: (CASE WHEN (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) = '0'::double precision) THEN '0'::double precision WHEN (((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision))) = '0'::double precision) THEN '1'::double precision ELSE (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) / ((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)))) END) DESC, b1.b_buildingkey, b2.b_buildingkey
        # -->         ->  Nested Loop  (cost=0.28..30839354.25 rows=392632 width=48)
        #               Output: b1.b_buildingkey, b2.b_buildingkey, st_area(b1.b_boundary), st_area(b2.b_boundary), st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)), CASE WHEN (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) = '0'::double precision) THEN '0'::double precision WHEN (((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision))) = '0'::double precision) THEN '1'::double precision ELSE (st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)) / ((st_area(b1.b_boundary) + st_area(b2.b_boundary)) - st_area(st_intersection(b1.b_boundary, b2.b_boundary, '-1'::double precision)))) END
        #               ->  Parallel Seq Scan on public.building b1  (cost=0.00..2443.46 rows=50846 width=143)
        #                     Output: b1.b_buildingkey, b1.b_name, b1.b_boundary
        #               ->  Index Scan using idx_building_b_boundary on public.building b2  (cost=0.28..113.07 rows=3 width=143)
        #                     Output: b2.b_buildingkey, b2.b_name, b2.b_boundary
        #                     Index Cond: (b2.b_boundary && b1.b_boundary)
        #                     Filter: ((b1.b_buildingkey < b2.b_buildingkey) AND st_intersects(b1.b_boundary, b2.b_boundary))
        return """
               -- Q9: Building Conflation (duplicate/overlap detection via IoU)
               WITH pairs AS (SELECT b1.b_buildingkey                                       AS building_1,
                                     b2.b_buildingkey                                       AS building_2,
                                     ST_Area(b1.b_boundary)                                 AS area1,
                                     ST_Area(b2.b_boundary)                                 AS area2,
                                     ST_Area(ST_Intersection(b1.b_boundary, b2.b_boundary)) AS overlap_area
                              FROM building b1
                                       JOIN building b2 ON b1.b_buildingkey < b2.b_buildingkey
                                  AND ST_Intersects(b1.b_boundary, b2.b_boundary))
               SELECT building_1,
                      building_2,
                      area1,
                      area2,
                      overlap_area,
                      CASE
                          WHEN overlap_area = 0 THEN 0.0
                          WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
                          ELSE overlap_area / (area1 + area2 - overlap_area)
                          END AS iou
               FROM pairs
               ORDER BY iou DESC, building_1 ASC, building_2 ASC
               """

    @staticmethod
    def q10() -> str:
# ==================== QUERY PLAN ====================
# Sort  (cost=17357605293.62..17357606430.39 rows=454710 width=79)
#   Output: z.z_zonekey, z.z_name, (avg((t.t_dropofftime - t.t_pickuptime))), (avg(t.t_distance)), (count(t.t_tripkey))
#   Sort Key: (avg((t.t_dropofftime - t.t_pickuptime))) DESC NULLS LAST, z.z_zonekey
#   ->  Finalize GroupAggregate  (cost=17357435994.83..17357562563.18 rows=454710 width=79)
#         Output: z.z_zonekey, z.z_name, avg((t.t_dropofftime - t.t_pickuptime)), avg(t.t_distance), count(t.t_tripkey)
#         Group Key: z.z_zonekey, z.z_name
#         ->  Gather Merge  (cost=17357435994.83..17357542101.23 rows=909420 width=95)
#               Output: z.z_zonekey, z.z_name, (PARTIAL avg((t.t_dropofftime - t.t_pickuptime))), (PARTIAL avg(t.t_distance)), (PARTIAL count(t.t_tripkey))
#               Workers Planned: 2
#               ->  Sort  (cost=17357434994.80..17357436131.58 rows=454710 width=95)
#                     Output: z.z_zonekey, z.z_name, (PARTIAL avg((t.t_dropofftime - t.t_pickuptime))), (PARTIAL avg(t.t_distance)), (PARTIAL count(t.t_tripkey))
#                     Sort Key: z.z_zonekey, z.z_name
#                     ->  Partial HashAggregate  (cost=17357386580.49..17357392264.37 rows=454710 width=95)
#                           Output: z.z_zonekey, z.z_name, PARTIAL avg((t.t_dropofftime - t.t_pickuptime)), PARTIAL avg(t.t_distance), PARTIAL count(t.t_tripkey)
#                           Group Key: z.z_zonekey, z.z_name
# -->                           ->  Nested Loop Left Join  (cost=0.42..17332113990.62 rows=1684839325 width=53)
#                                 Output: z.z_zonekey, z.z_name, t.t_dropofftime, t.t_pickuptime, t.t_distance, t.t_tripkey
#                                 ->  Parallel Seq Scan on public.zone z  (cost=0.00..96320.62 rows=189462 width=4544)
#                                       Output: z.z_zonekey, z.z_gersid, z.z_country, z.z_region, z.z_name, z.z_subtype, z.z_boundary
#                                 ->  Index Scan using idx_trip_t_pickuploc on public.trip t  (cost=0.42..91420.18 rows=6000 width=62)
#                                       Output: t.t_tripkey, t.t_custkey, t.t_driverkey, t.t_vehiclekey, t.t_pickuptime, t.t_dropofftime, t.t_fare, t.t_tip, t.t_totalamount, t.t_distance, t.t_pickuploc, t.t_dropoffloc
#                                       Index Cond: (t.t_pickuploc @ z.z_boundary)
#                                       Filter: st_within(t.t_pickuploc, z.z_boundary)
# ======================================================
        return """
               -- Q10: Zone statistics for trips starting within each zone
               SELECT z.z_zonekey,
                      z.z_name                              AS pickup_zone,
                      AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
                      AVG(t.t_distance)                     AS avg_distance,
                      COUNT(t.t_tripkey)                    AS num_trips
               FROM zone z
                            LEFT JOIN trip t ON ST_Within(t.t_pickuploc, z.z_boundary)
               GROUP BY z.z_zonekey, z.z_name
               ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
               """

    @staticmethod
    def q11() -> str:
        # ==================== QUERY PLAN ====================
        # Finalize Aggregate  (cost=1163008919327.46..1163008919327.47 rows=1 width=8)
        #   Output: count(*)
        #   ->  Gather  (cost=1163008919327.25..1163008919327.46 rows=2 width=8)
        #         Output: (PARTIAL count(*))
        #         Workers Planned: 2
        #         ->  Partial Aggregate  (cost=1163008918327.25..1163008918327.26 rows=1 width=8)
        #               Output: PARTIAL count(*)
        # -->               ->  Nested Loop  (cost=0.57..1162442714551.25 rows=226481510401 width=0)
        #                     Join Filter: (pickup_zone.z_zonekey <> dropoff_zone.z_zonekey)
        # -->                     ->  Nested Loop  (cost=0.29..16995052284.67 rows=1684839325 width=40)
        #                           Output: t.t_dropoffloc, pickup_zone.z_zonekey
        #                           ->  Parallel Seq Scan on public.trip t  (cost=0.00..1471638.00 rows=25000000 width=64)
        #                                 Output: t.t_tripkey, t.t_custkey, t.t_driverkey, t.t_vehiclekey, t.t_pickuptime, t.t_dropofftime, t.t_fare, t.t_tip, t.t_totalamount, t.t_distance, t.t_pickuploc, t.t_dropoffloc
        #                           ->  Index Scan using idx_zone_z_boundary on public.zone pickup_zone  (cost=0.29..679.29 rows=45 width=4529)
        #                                 Output: pickup_zone.z_zonekey, pickup_zone.z_gersid, pickup_zone.z_country, pickup_zone.z_region, pickup_zone.z_name, pickup_zone.z_subtype, pickup_zone.z_boundary
        #                                 Index Cond: (pickup_zone.z_boundary ~ t.t_pickuploc)
        #                                 Filter: st_within(t.t_pickuploc, pickup_zone.z_boundary)
        #                     ->  Index Scan using idx_zone_z_boundary on public.zone dropoff_zone  (cost=0.29..679.29 rows=45 width=4529)
        #                           Output: dropoff_zone.z_zonekey, dropoff_zone.z_gersid, dropoff_zone.z_country, dropoff_zone.z_region, dropoff_zone.z_name, dropoff_zone.z_subtype, dropoff_zone.z_boundary
        #                           Index Cond: (dropoff_zone.z_boundary ~ t.t_dropoffloc)
        #                           Filter: st_within(t.t_dropoffloc, dropoff_zone.z_boundary)
        return """
               -- Q11: Count trips that cross between different zones
               SELECT COUNT(*) AS cross_zone_trip_count
               FROM trip t
                        JOIN zone pickup_zone ON ST_Within(t.t_pickuploc, pickup_zone.z_boundary)
                        JOIN zone dropoff_zone ON ST_Within(t.t_dropoffloc, dropoff_zone.z_boundary)
               WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
               """

    @staticmethod
    def q12() -> str:
        return """
               -- Q12 (PG-Strom): KNN using CROSS JOIN LATERAL and <-> operator
               SELECT t.t_tripkey,
                      t.t_pickuploc,
                      nb.b_buildingkey,
                      nb.b_name AS building_name,
                      nb.distance_to_building
               FROM trip t
                        CROSS JOIN LATERAL (
                   SELECT b.b_buildingkey,
                          b.b_name,
                          ST_Distance(t.t_pickuploc, b.b_boundary) AS distance_to_building
                   FROM building b
                    ORDER BY t.t_pickuploc <-> b.b_boundary
                       LIMIT 5
) AS nb
               ORDER BY nb.distance_to_building ASC, nb.b_buildingkey ASC
               """


class PostGISSpatialBenchBenchmark(PgStromSpatialBenchBenchmark):
    """A PostGIS-specific implementation of the SpatialBench benchmark."""

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "PostGIS"


def main():
    query_classes = {
        "SedonaSpark": SpatialBenchBenchmark,
        "Databricks": DatabricksSpatialBenchBenchmark,
        "DuckDB": DuckDBSpatialBenchBenchmark,
        "SedonaDB": SedonaDBSpatialBenchBenchmark,
        "PgStrom": PgStromSpatialBenchBenchmark,
        "PostGIS": PostGISSpatialBenchBenchmark,
        "Geopandas": None,  # Special case, we will catch this below,
        "Spatial Polars": None,  # Special case, we will catch this below,
    }

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <dialect>")
        print(f"Available dialects: {', '.join(query_classes.keys())}")
        sys.exit(1)

    dialect_arg = sys.argv[1]

    if dialect_arg in ["Geopandas", "Spatial Polars"]:
        dialect_script_name = dialect_arg.lower().replace(" ", "_")
        print(
            f"{dialect_arg} does not support SQL queries directly. Please use the provided Python script {dialect_script_name}.py.")
        sys.exit(0)

    if dialect_arg not in query_classes:
        print(f"Unknown dialect: {dialect_arg}")
        print(f"Available dialects: {', '.join(query_classes.keys())}")
        sys.exit(1)

    queries = query_classes[dialect_arg]().queries()

    for query in queries.values():
        print(query)


if __name__ == "__main__":
    main()
