CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN PIPELINE
-- Q12 (ClickHouse): Nearest buildings to trip pickups (KNN join)
WITH trip_sampled AS (
   SELECT t_tripkey, readWKBPoint(t_pickuploc) AS pt
   FROM trip
),
buildings AS (
   SELECT b_buildingkey, readWKBPolygon(b_boundary) AS boundary
   FROM building
)
SELECT
   t.t_tripkey,
   AVG(b.dist) AS avg_distance_to_5_nearest
FROM (
   SELECT
       t.t_tripkey,
       polygonsDistanceCartesian(b.boundary, [[t.pt]]) AS dist,
       row_number() OVER (PARTITION BY t.t_tripkey ORDER BY dist ASC) AS rn
   FROM trip_sampled t
   CROSS JOIN buildings b
) ranked
WHERE rn <= 5
GROUP BY t_tripkey
ORDER BY avg_distance_to_5_nearest DESC, t_tripkey ASC
LIMIT 100;
