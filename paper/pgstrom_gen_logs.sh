#!/bin/bash

# Configuration
DB_NAME=${1:-"spatialbench"} # Default DB name is 'spatialbench'
SF=${2:-"1"}                 # Default Scale Factor is 1
DB_HOST="127.0.0.1"          # Your custom host
DB_PORT="5433"               # Your custom port
LOG_DIR="logs/sf${SF}"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "========================================================="
echo " Starting PG-Strom Benchmarks for Scale Factor: $SF"
echo " Connection: host=$DB_HOST, port=$DB_PORT, db=$DB_NAME"
echo " Output Directory: $LOG_DIR"
echo "========================================================="

# Function to execute and log a query
run_query() {
    local Q_NUM=$1
    local FULL_SQL=$2
    local FILTER_SQL=$3
    local LOG_FILE="$LOG_DIR/pgstrom_sf${SF}_q${Q_NUM}_results.log"

    echo "Running Q${Q_NUM}..."

    # Step 1: Warm up the cache (run the full query once without saving to avoid disk I/O skew)
    echo "  -> Warming up cache..."
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -c "$FULL_SQL" > /dev/null 2>&1

    # Step 2: Execute both queries in the same session with I/O tracking enabled
    echo "  -> Executing EXPLAIN ANALYZE and saving logs..."
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" > "$LOG_FILE" <<EOF
SET track_io_timing = on;

-- 1. Full Query Execution
EXPLAIN (ANALYZE, BUFFERS)
$FULL_SQL

-- 2. Filter-Only Execution
EXPLAIN ANALYZE
$FILTER_SQL
EOF

    echo "  -> Saved to $LOG_FILE"
}

# ==========================================
# SQL DEFINITIONS
# ==========================================

# Q2
Q2_FULL="
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(t.t_pickuploc, (SELECT z.z_boundary FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1));"

Q2_FILTER="
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE t.t_pickuploc && (SELECT z.z_boundary FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1);"


# Q4
Q4_FULL="
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z
JOIN (SELECT t.t_pickuploc FROM trip t ORDER BY t.t_tip DESC, t.t_tripkey ASC LIMIT 1000) top_trips
    ON ST_Within(top_trips.t_pickuploc, z.z_boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC;"

Q4_FILTER="
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z
JOIN (SELECT t.t_pickuploc FROM trip t ORDER BY t.t_tip DESC, t.t_tripkey ASC LIMIT 1000) top_trips
    ON top_trips.t_pickuploc && z.z_boundary
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC;"


# Q6
Q6_FULL="
SELECT z.z_zonekey, z.z_name, COUNT(t.t_tripkey) AS total_pickups,
       AVG(t.t_totalamount) AS avg_distance, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))', 4326), z.z_boundary)
  AND ST_Within(t.t_pickuploc, z.z_boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC;"

Q6_FILTER="
SELECT z.z_zonekey, z.z_name, COUNT(t.t_tripkey) AS total_pickups,
       AVG(t.t_totalamount) AS avg_distance, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))', 4326) && z.z_boundary
  AND t.t_pickuploc && z.z_boundary
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC;"


# Q9
Q9_FULL="
WITH pairs AS (
    SELECT b1.b_buildingkey AS building_1, b2.b_buildingkey AS building_2,
           ST_Area(b1.b_boundary) AS area1, ST_Area(b2.b_boundary) AS area2,
           ST_Area(ST_Intersection(b1.b_boundary, b2.b_boundary)) AS overlap_area
    FROM building b1
    JOIN building b2 ON b1.b_buildingkey < b2.b_buildingkey AND ST_Intersects(b1.b_boundary, b2.b_boundary)
)
SELECT building_1, building_2, area1, area2, overlap_area,
       CASE WHEN overlap_area = 0 THEN 0.0
            WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
            ELSE overlap_area / (area1 + area2 - overlap_area)
       END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC;"

Q9_FILTER="
WITH pairs AS (
    SELECT b1.b_buildingkey AS building_1, b2.b_buildingkey AS building_2
    FROM building b1
    JOIN building b2 ON b1.b_buildingkey < b2.b_buildingkey AND b1.b_boundary && b2.b_boundary
)
SELECT COUNT(*) FROM pairs;"


# Q10
Q10_FULL="
SELECT z.z_zonekey, z.z_name AS pickup_zone,
       AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z
LEFT JOIN trip t ON ST_Within(t.t_pickuploc, z.z_boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC;"

Q10_FILTER="
SELECT z.z_zonekey, z.z_name AS pickup_zone,
       AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z
LEFT JOIN trip t ON t.t_pickuploc && z.z_boundary
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC;"


# Q11
Q11_FULL="
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(t.t_pickuploc, pickup_zone.z_boundary)
JOIN zone dropoff_zone ON ST_Within(t.t_dropoffloc, dropoff_zone.z_boundary)
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey;"

Q11_FILTER="
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON t.t_pickuploc && pickup_zone.z_boundary
JOIN zone dropoff_zone ON t.t_dropoffloc && dropoff_zone.z_boundary
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey;"


# ==========================================
# EXECUTE ALL BENCHMARKS
# ==========================================

run_query "2" "$Q2_FULL" "$Q2_FILTER"
run_query "4" "$Q4_FULL" "$Q4_FILTER"
run_query "6" "$Q6_FULL" "$Q6_FILTER"
run_query "9" "$Q9_FULL" "$Q9_FILTER"
run_query "10" "$Q10_FULL" "$Q10_FILTER"
run_query "11" "$Q11_FULL" "$Q11_FILTER"

echo "========================================================="
echo " Benchmarks Complete! Logs are saved in: $LOG_DIR/"
echo "========================================================="
