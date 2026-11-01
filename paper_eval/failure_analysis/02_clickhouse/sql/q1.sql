CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
-- Q1 (ClickHouse): Find trips starting within 50km of Sedona city center
SELECT
   t.t_tripkey,
   readWKBPoint(t.t_pickuploc).1 AS pickup_lon,
   readWKBPoint(t.t_pickuploc).2 AS pickup_lat,
   t.t_pickuptime,
   L2Distance(readWKBPoint(t.t_pickuploc), (-111.7610, 34.8697)) AS distance_to_center
FROM trip t
WHERE L2Distance(readWKBPoint(t.t_pickuploc), (-111.7610, 34.8697)) <= 0.45
ORDER BY distance_to_center ASC, t.t_tripkey ASC
LIMIT 100;
