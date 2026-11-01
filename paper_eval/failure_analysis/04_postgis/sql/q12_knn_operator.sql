-- Q12 (PostGIS) rewrite, diagnostic: identical semantics, but the LATERAL subquery
-- orders by the KNN distance operator <-> on the indexed expression, so PostgreSQL
-- can use an index-ordered GiST scan (idx_building_geom) and stop after 5 rows,
-- instead of computing ST_Distance to all 86k buildings and sorting, per trip.
-- (For geometry, PostGIS >= 2.2 on PG >= 9.5 rechecks <-> with the exact distance.)
SELECT
   t.t_tripkey,
   AVG(nb.distance_to_building) AS avg_distance_to_5_nearest
FROM trip t
CROSS JOIN LATERAL (
   SELECT ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) AS distance_to_building
   FROM building b
   ORDER BY ST_GeomFromWKB(b.b_boundary) <-> ST_GeomFromWKB(t.t_pickuploc)
   LIMIT 5
) AS nb
GROUP BY t.t_tripkey
ORDER BY avg_distance_to_5_nearest DESC, t.t_tripkey ASC
LIMIT 100
