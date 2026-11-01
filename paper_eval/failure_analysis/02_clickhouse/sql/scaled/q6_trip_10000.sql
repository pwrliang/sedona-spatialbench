CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet') LIMIT 10000;
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
-- Q6 (ClickHouse): Zone statistics for trips intersecting a bounding box
WITH filtered_zones AS (
    SELECT z_zonekey, z_name, readWKB(z_boundary) AS boundary
    FROM zone
    WHERE geometryIntersectCartesian(
        readWKT('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'),
        readWKB(z_boundary)
    )
)
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups,
   AVG(t.t_distance) AS avg_distance,
   AVG(dateDiff('second', t.t_pickuptime, t.t_dropofftime)) AS avg_duration
FROM trip t
JOIN filtered_zones z ON geometryIntersectCartesian(readWKB(t.t_pickuploc), z.boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC;
