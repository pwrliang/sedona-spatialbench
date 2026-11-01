CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet') LIMIT 10000;
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
-- Q11 (ClickHouse): Count trips that cross between different zones
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON geometryIntersectCartesian(readWKB(t.t_pickuploc), readWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON geometryIntersectCartesian(readWKB(t.t_dropoffloc), readWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey;
