-- Q10 (PostGIS) on a deterministic 1% sample of zones (z_zonekey % 100 = 0):
-- plan is "for each zone: GiST index scan of trip with the zone bbox, then ST_Within".
SELECT z.z_zonekey, z.z_name AS pickup_zone, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM (SELECT * FROM zone WHERE z_zonekey % 100 = 0) z
LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
LIMIT 100
