-- Q9 rewrite attempt (diagnostic): inequality moved from ON to WHERE.
-- The optimizer pushes it back into the join condition -> still no SPATIAL_JOIN.
WITH b AS (SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom FROM building),
pairs AS (
    SELECT b1.id AS building_1, b2.id AS building_2, ST_Area(b1.geom) AS area1, ST_Area(b2.geom) AS area2,
           ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
    FROM b b1 JOIN b b2 ON ST_Intersects(b1.geom, b2.geom)
    WHERE b1.id < b2.id
)
SELECT building_1, building_2, area1, area2, overlap_area,
   CASE WHEN overlap_area = 0 THEN 0.0
        WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
        ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
LIMIT 100
