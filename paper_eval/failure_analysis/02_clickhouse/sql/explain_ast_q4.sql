CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN SYNTAX
-- Q4 (ClickHouse): Zone distribution of top 1000 trips by tip amount
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM (SELECT z_zonekey, z_name, readWKB(z_boundary) AS boundary FROM zone) z
JOIN (
    SELECT readWKB(t_pickuploc) AS pt
    FROM trip
    ORDER BY t_tip DESC, t_tripkey ASC
    LIMIT 1000
) top_trips ON geometryIntersectCartesian(top_trips.pt, z.boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC;
