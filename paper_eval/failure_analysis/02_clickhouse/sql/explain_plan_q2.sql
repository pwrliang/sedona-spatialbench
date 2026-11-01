CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN PLAN actions=1, header=0
-- Q2 (ClickHouse): Count trips starting within Coconino County zone
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE pointInPolygon(
    readWKBPoint(t.t_pickuploc),
    (SELECT readWKBPolygon(z_boundary) FROM zone WHERE z_name = 'Coconino County' LIMIT 1)
);
