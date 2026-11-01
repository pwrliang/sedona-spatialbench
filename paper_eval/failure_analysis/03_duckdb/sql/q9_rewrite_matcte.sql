-- Q9 rewrite (diagnostic only, semantically identical to the benchmark query):
-- the spatial join is isolated in a MATERIALIZED CTE whose join condition is ONLY
-- ST_Intersects, so DuckDB's spatial-join rewrite can fire; the b1.id < b2.id
-- predicate is applied afterwards.
WITH b AS (SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom FROM building),
cand AS MATERIALIZED (
    SELECT b1.id AS building_1, b2.id AS building_2, b1.geom AS g1, b2.geom AS g2
    FROM b b1 JOIN b b2 ON ST_Intersects(b1.geom, b2.geom)
),
pairs AS (
    SELECT building_1, building_2, ST_Area(g1) AS area1, ST_Area(g2) AS area2,
           ST_Area(ST_Intersection(g1, g2)) AS overlap_area
    FROM cand WHERE building_1 < building_2
)
SELECT building_1, building_2, area1, area2, overlap_area,
   CASE WHEN overlap_area = 0 THEN 0.0
        WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
        ELSE overlap_area / (area1 + area2 - overlap_area) END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
LIMIT 100
