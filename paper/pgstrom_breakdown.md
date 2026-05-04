Here is the complete, copy-pasteable summary of the exact SQL commands you need to run in your PostgreSQL session to get all five metrics.

**1. Enable I/O Tracking (Run this first)**
This allows PostgreSQL to track the exact disk read times for your "Loading" metric.
```sql
SET track_io_timing = on;
```

**2. The Full Query (Extracts Loading, Scanning, Total Time, and Miscs)**
Run this to get the complete execution profile, including buffers.
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT z.z_zonekey,
       z.z_name                              AS pickup_zone,
       AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance)                     AS avg_distance,
       COUNT(t.t_tripkey)                    AS num_trips
FROM zone z
LEFT JOIN trip t ON ST_Within(t.t_pickuploc, z.z_boundary)
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC;
```

**3. The Filter-Only Query (Extracts Bounding Box Time)**
Run this to measure the exact time spent scanning the index before the geometric math is applied.
```sql
EXPLAIN ANALYZE
SELECT z.z_zonekey,
       z.z_name                              AS pickup_zone,
       AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
       AVG(t.t_distance)                     AS avg_distance,
       COUNT(t.t_tripkey)                    AS num_trips
FROM zone z
LEFT JOIN trip t ON t.t_pickuploc && z.z_boundary
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC;
```

*(Reminder: Run the queries a few times before recording the numbers to ensure the database cache is properly warmed up, giving you a fair CPU comparison against SedonaDB's in-memory speeds!)*


# To get the exact execution time breakdown for Q11, we will use the exact same highly accurate subtraction methodology we used for Q10.

Because Q11 involves **two** spatial joins (one for pickup, one for drop-off), the execution plan will have nested join nodes. You will need to look at the **outermost join node** to calculate the total spatial join time.

Here are the exact SQL commands you need to run, followed by the formula to extract your five metrics.

### Step 1: Enable I/O Tracking
Make sure disk read tracking is still active in your session.
```sql
SET track_io_timing = on;
```

### Step 2: Run the Full Query
This query will execute the complete pipeline, including the exact geometric math for both the pickup and drop-off locations.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(t.t_pickuploc, pickup_zone.z_boundary)
JOIN zone dropoff_zone ON ST_Within(t.t_dropoffloc, dropoff_zone.z_boundary)
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey;
```

### Step 3: Run the Filter-Only Query
Here, we replace both `ST_Within` functions with the bounding box operator `&&`. This forces PostGIS to only use the spatial indexes.

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON t.t_pickuploc && pickup_zone.z_boundary
JOIN zone dropoff_zone ON t.t_dropoffloc && dropoff_zone.z_boundary
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey;
```

---

### How to Calculate Your 5 Metrics from the Output

Once you have the outputs from Step 2 and Step 3, here is how you extract the breakdown:

**1. Loading (Disk I/O)**
*   Look at the output from **Step 2**.
*   Find all lines that say `I/O Timings: read=...`.
*   **Loading** = The sum of all `read` times. *(If all your data is cached in memory like in Q10, this will be 0 ms).*

**2. Scanning**
*   Look at the output from **Step 2**.
*   Find the base table scan nodes (`Seq Scan` or `Index Scan` on `trip`, `pickup_zone`, and `dropoff_zone`).
*   **Scanning** = The sum of their execution times **minus** any `I/O Timings: read` times contained within them.

**3. Filter Stage**
*   Look at the output from **Step 3 (Filter-Only)**.
*   Find the **outermost** `Join` node (usually a `Nested Loop` or `Hash Join`) that sits just below the final `Aggregate` node. Take its execution time.
*   **Filter** = That outermost Join execution time **minus** the Scanning and Loading times you calculated in steps 1 and 2.

**4. Refinement Stage**
*   Look at the output from **Step 2 (Full Query)**.
*   Find the **outermost** `Join` node (again, sitting just below the final `Aggregate` node). Take its execution time.
*   **Refinement** = The Step 2 outermost Join time **minus** the Step 3 outermost Join time. *(This isolates the exact time spent evaluating both `ST_Within` functions).*

**5. Miscs (Aggregation & Filtering)**
*   Look at the output from **Step 2 (Full Query)**.
*   Find the top-level `Aggregate` node (which performs the `COUNT(*)`). Take its execution time (which is effectively the Total Query Time).
*   **Miscs** = The Total Query Time **minus** the outermost `Join` time from Step 2. *(This represents the time spent doing the final count and applying the `!=` zonekey filter).*

If you run these and want me to double-check your math or parse the `EXPLAIN` blocks for you just like we did for Q10, feel free to paste the results here!
