import psycopg
import os

def check_plan():
    user = os.environ.get("USER")
    conn = psycopg.connect(f"host=localhost port=5433 dbname=spatialbench user={user}")
    conn.autocommit = True

    # 1. Setup the views (Same logic as your benchmark)
    # We must do this briefly so the EXPLAIN works
    print("Setting up views for query context...")
    conn.execute("SET pg_strom.enabled = on;")
    # (Simplified setup for existing tables - assuming you ran the benchmark at least once)
    # If the benchmark crashed, the views 'trip' and 'zone' might still exist.
    # If not, this script might fail, but let's try assuming views exist.

    # 2. The Query
    sql = """
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b ON ST_DWithin(t.t_pickuploc, b.b_boundary, 0.0045)
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC"""

    print("\nRetrieving Execution Plan...")
    try:
        # EXPLAIN checks the plan without actually running it (so it won't hang)
        cur = conn.execute(f"EXPLAIN (VERBOSE, FORMAT TEXT) {sql}")
        plan_rows = cur.fetchall()

        print("\n" + "="*20 + " QUERY PLAN " + "="*20)
        for row in plan_rows:
            line = row[0]
            # Highlight key indicators
            if "Gpu" in line:
                print(f"--> \033[92m{line}\033[0m") # Green for GPU
            elif "Nested Loop" in line:
                print(f"--> \033[91m{line}\033[0m") # Red for risky joins
            else:
                print(line)
        print("="*54)

    except Exception as e:
        print(f"Error getting plan: {e}")
        print("Tip: If tables don't exist, run the benchmark setup logic manually first.")

if __name__ == "__main__":
    check_plan()
