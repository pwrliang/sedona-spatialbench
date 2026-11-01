-- Validity of the building polygons stored in PostGIS (GEOS 3.8 validity rules)
SELECT count(*) AS buildings, count(*) FILTER (WHERE NOT ST_IsValid(b_boundary)) AS invalid FROM building;
SELECT reason, count(*) FROM (SELECT split_part(ST_IsValidReason(b_boundary), '[', 1) AS reason FROM building WHERE NOT ST_IsValid(b_boundary)) s GROUP BY 1 ORDER BY 2 DESC;
SELECT b_buildingkey, ST_IsValidReason(b_boundary), ST_AsText(b_boundary) FROM building WHERE NOT ST_IsValid(b_boundary) ORDER BY b_buildingkey LIMIT 3;
SELECT count(*) AS hf_buildings, count(*) FILTER (WHERE NOT ST_IsValid(b_boundary)) AS hf_invalid FROM hf.building;
