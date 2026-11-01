-- Q10 core (diagnostic): same join, but without the 38,679 'county' polygons
-- (avg 2,029 vertices, max 345k) -> sensitivity of DuckDB's refinement step to polygon complexity.
SELECT count(*) FROM zone z JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
WHERE z.z_subtype <> 'county'
