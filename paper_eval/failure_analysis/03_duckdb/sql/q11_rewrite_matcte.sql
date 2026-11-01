-- Q11 rewrite (diagnostic only, semantically identical): each spatial join is a
-- separate MATERIALIZED CTE with only the ST_Within predicate; the two are then
-- combined with an equi-join on t_tripkey and the != filter.
WITH pu AS MATERIALIZED (
    SELECT t.t_tripkey, z.z_zonekey FROM trip t JOIN zone z
      ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))),
dz AS MATERIALIZED (
    SELECT t.t_tripkey, z.z_zonekey FROM trip t JOIN zone z
      ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(z.z_boundary)))
SELECT COUNT(*) AS cross_zone_trip_count
FROM pu JOIN dz ON pu.t_tripkey = dz.t_tripkey
WHERE pu.z_zonekey != dz.z_zonekey
