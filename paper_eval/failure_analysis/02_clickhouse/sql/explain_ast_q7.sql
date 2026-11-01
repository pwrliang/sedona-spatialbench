CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN SYNTAX
-- Q7 (ClickHouse): Route detours by comparing reported vs geometric distance
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance AS reported_distance_m,
       L2Distance(readWKBPoint(t.t_pickuploc), readWKBPoint(t.t_dropoffloc)) / 0.000009 AS line_distance_m
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   if(t.line_distance_m = 0, NULL, t.reported_distance_m / t.line_distance_m) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
LIMIT 100;
