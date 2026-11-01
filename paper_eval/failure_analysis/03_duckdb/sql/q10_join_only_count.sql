-- Q10 core (diagnostic): just the spatial join cardinality, no aggregation by zone.
SELECT count(*) FROM zone z JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
