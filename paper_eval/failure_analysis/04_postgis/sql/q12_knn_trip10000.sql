-- Q12 KNN-operator rewrite on a 10000-trip prefix
SELECT t.t_tripkey, AVG(nb.distance_to_building) AS avg_distance_to_5_nearest
FROM (SELECT * FROM trip ORDER BY t_tripkey LIMIT 10000) t
CROSS JOIN LATERAL (
   SELECT ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) AS distance_to_building
   FROM building b ORDER BY ST_GeomFromWKB(b.b_boundary) <-> ST_GeomFromWKB(t.t_pickuploc) LIMIT 5) AS nb
GROUP BY t.t_tripkey ORDER BY avg_distance_to_5_nearest DESC, t.t_tripkey ASC LIMIT 100
