CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN PIPELINE
-- Q3 (ClickHouse): Monthly trip statistics for a buffered box around Sedona
SELECT
   toStartOfMonth(t.t_pickuptime) AS pickup_month,
   COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance,
   AVG(dateDiff('second', t.t_pickuptime, t.t_dropofftime)) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE readWKBPoint(t.t_pickuploc).1 BETWEEN -111.9510 AND -111.5710
  AND readWKBPoint(t.t_pickuploc).2 BETWEEN 34.6897 AND 35.0497
GROUP BY pickup_month
ORDER BY pickup_month;
