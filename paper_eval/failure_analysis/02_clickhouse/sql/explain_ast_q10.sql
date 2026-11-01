CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN SYNTAX
-- Q10 (ClickHouse): Zone statistics for trips starting within each zone
SELECT
   z.z_zonekey,
   z.z_name AS pickup_zone,
   AVG(dateDiff('second', t.t_pickuptime, t.t_dropofftime)) AS avg_duration,
   AVG(t.t_distance) AS avg_distance,
   COUNT(t.t_tripkey) AS num_trips
FROM zone z
LEFT JOIN trip t ON geometryIntersectCartesian(readWKB(t.t_pickuploc), readWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
LIMIT 100;
