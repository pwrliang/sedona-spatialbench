CREATE VIEW IF NOT EXISTS building AS SELECT * FROM file('/local/storage/liang/sf10/building/*.parquet');
CREATE VIEW IF NOT EXISTS customer AS SELECT * FROM file('/local/storage/liang/sf10/customer/*.parquet');
CREATE VIEW IF NOT EXISTS driver AS SELECT * FROM file('/local/storage/liang/sf10/driver/*.parquet');
CREATE VIEW IF NOT EXISTS trip AS SELECT * FROM file('/local/storage/liang/sf10/trip/*.parquet');
CREATE VIEW IF NOT EXISTS vehicle AS SELECT * FROM file('/local/storage/liang/sf10/vehicle/*.parquet');
CREATE VIEW IF NOT EXISTS zone AS SELECT * FROM file('/local/storage/liang/sf10/zone/*.parquet');
EXPLAIN PIPELINE
-- Q5 (ClickHouse): Monthly travel patterns for repeat customers (convex hull of dropoffs)
SELECT
   c.c_custkey,
   c.c_name AS customer_name,
   toStartOfMonth(t.t_pickuptime) AS pickup_month,
   abs(polygonAreaCartesian(polygonConvexHullCartesian(groupArray(readWKBPoint(t.t_dropoffloc))))) AS monthly_travel_hull_area,
   COUNT(*) AS dropoff_count
FROM trip t
JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC
LIMIT 100;
