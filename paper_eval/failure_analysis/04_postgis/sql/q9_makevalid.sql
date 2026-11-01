-- Q9 (PostGIS) diagnostic: same query, building polygons repaired with ST_MakeValid
-- (GEOS 3.8 overlay throws TopologyException on the 374 invalid polygons of the dataset copy used)
WITH b1 AS (SELECT b_buildingkey AS id, ST_MakeValid(ST_GeomFromWKB(b_boundary)) AS geom FROM building),
     b2 AS (SELECT b_buildingkey AS id, ST_MakeValid(ST_GeomFromWKB(b_boundary)) AS geom FROM building),
     pairs AS (
        SELECT b1.id AS building_1, b2.id AS building_2, ST_Area(b1.geom) AS area1, ST_Area(b2.geom) AS area2,
               ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
        FROM b1 JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom))
SELECT building_1, building_2, area1, area2, overlap_area,
   CASE WHEN overlap_area = 0 THEN 0.0 WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
        ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
FROM pairs ORDER BY iou DESC, building_1 ASC, building_2 ASC LIMIT 100
