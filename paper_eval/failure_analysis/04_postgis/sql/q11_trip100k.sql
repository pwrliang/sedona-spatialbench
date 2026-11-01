-- Q11 (PostGIS) on a 100k-trip prefix: per-trip cost of the plan
-- "seq scan trip -> GiST probe zone (pickup) -> GiST probe zone (dropoff)"
SELECT COUNT(*) AS cross_zone_trip_count
FROM (SELECT * FROM trip ORDER BY t_tripkey LIMIT 100000) t
JOIN zone pickup_zone ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
