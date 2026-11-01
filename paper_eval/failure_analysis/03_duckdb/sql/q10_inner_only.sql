-- Q10 variant (diagnostic): INNER instead of LEFT join, to test whether the outer
-- join type matters for the spatial join operator (result differs only by zones without trips).
SELECT z.z_zonekey, z.z_name AS pickup_zone, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
LIMIT 100
