-- Q10 core (diagnostic): only the 'county' polygons.
SELECT count(*) FROM zone z JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
WHERE z.z_subtype = 'county'
