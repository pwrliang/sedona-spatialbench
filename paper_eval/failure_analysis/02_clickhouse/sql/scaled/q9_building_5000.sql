CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet') LIMIT 5000;
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
-- Q9 (ClickHouse): Building Conflation (duplicate/overlap detection via IoU)
WITH b1 AS (
   SELECT b_buildingkey AS id, readWKBPolygon(b_boundary) AS geom
   FROM building
),
b2 AS (
   SELECT b_buildingkey AS id, readWKBPolygon(b_boundary) AS geom
   FROM building
),
pairs AS (
   SELECT
       b1.id AS building_1,
       b2.id AS building_2,
       abs(polygonAreaCartesian(b1.geom)) AS area1,
       abs(polygonAreaCartesian(b2.geom)) AS area2,
       abs(polygonAreaCartesian(polygonsIntersectionCartesian(b1.geom, b2.geom))) AS overlap_area
   FROM b1
   JOIN b2 ON b1.id < b2.id AND polygonsIntersectCartesian(b1.geom, b2.geom)
)
SELECT
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   if((area1 + area2 - overlap_area) = 0, 0.0, overlap_area / (area1 + area2 - overlap_area)) AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
LIMIT 100;
