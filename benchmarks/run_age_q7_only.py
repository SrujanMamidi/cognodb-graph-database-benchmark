import os
import sys
import time
import json
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import psycopg2

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.seed import get_random_node_pairs
from benchmarks.config import CONCURRENCY_WORKERS, TOTAL_Q7_OPERATIONS, READ_RATIO, RESULTS_FILE
from benchmarks.metrics import compute_latency_stats
from benchmarks.runners.age_runner import get_age_connection, AGE_GRAPH

load_dotenv()


def run_age_q7_standalone():
    print("=== Standalone Apache AGE Q7 Execution ===")
    conn = get_age_connection()

    # Step 1: Pre-execution verification
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper) RETURN count(p) $$) AS (c agtype);")
        pre_nodes = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:CITES]->() RETURN count(r) $$) AS (c agtype);")
        pre_cites = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:TEMP_CITES]->() RETURN count(r) $$) AS (c agtype);")
        pre_temp = int(str(cur.fetchone()[0]).strip('"'))

    print(f"Pre-check: Nodes={pre_nodes:,}, CITES={pre_cites:,}, TEMP_CITES={pre_temp}")
    assert pre_nodes == 34546, f"Expected 34,546 nodes, got {pre_nodes}"
    assert pre_cites == 421578, f"Expected 421,578 CITES relationships, got {pre_cites}"
    assert pre_temp == 0, f"Expected 0 TEMP_CITES relationships, got {pre_temp}"

    # Step 2: Run Q7 Workload
    print(f"Running Apache AGE Q7 ({CONCURRENCY_WORKERS} workers, {TOTAL_Q7_OPERATIONS} ops, 80/20 ratio)...")
    node_pairs = get_random_node_pairs(count=TOTAL_Q7_OPERATIONS)
    q7_latencies_ms = []
    errors = 0
    read_count = 0
    write_count = 0

    for i in range(TOTAL_Q7_OPERATIONS):
        if (i % 100) >= int(READ_RATIO * 100):
            write_count += 1
        else:
            read_count += 1

    def worker_task(task_id: int):
        src, tgt = node_pairs[task_id % len(node_pairs)]
        is_write = (task_id % 100) >= int(READ_RATIO * 100)
        local_conn = get_age_connection()
        t0 = time.perf_counter()
        try:
            with local_conn.cursor() as local_cur:
                if is_write:
                    query = (
                        f"SELECT * FROM cypher('{AGE_GRAPH}', $$ "
                        f"MATCH (s:Paper {{id: {src}}}), (t:Paper {{id: {tgt}}}) "
                        f"CREATE (s)-[:TEMP_CITES {{worker: {task_id}, ts: {time.time()}}}]->(t) "
                        "$$) AS (a agtype);"
                    )
                    local_cur.execute(query)
                else:
                    query = f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper {{id: {src}}})-[:CITES]->(m:Paper) RETURN count(m) $$) AS (c agtype);"
                    local_cur.execute(query)
                    local_cur.fetchall()
            local_conn.close()
            return (time.perf_counter() - t0) * 1000.0, None
        except Exception as e:
            try:
                local_conn.close()
            except Exception:
                pass
            return (time.perf_counter() - t0) * 1000.0, str(e)

    q7_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CONCURRENCY_WORKERS) as executor:
        futures = [executor.submit(worker_task, i) for i in range(TOTAL_Q7_OPERATIONS)]
        for fut in as_completed(futures):
            lat, err = fut.result()
            q7_latencies_ms.append(lat)
            if err:
                errors += 1
    q7_wall_time = time.perf_counter() - q7_start

    # Step 3: Mandatory Cleanup
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:TEMP_CITES]->() DELETE r $$) AS (a agtype);")
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper) RETURN count(p) $$) AS (c agtype);")
        post_nodes = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:CITES]->() RETURN count(r) $$) AS (c agtype);")
        post_cites = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:TEMP_CITES]->() RETURN count(r) $$) AS (c agtype);")
        post_temp = int(str(cur.fetchone()[0]).strip('"'))

    conn.close()

    print(f"Post-check: Nodes={post_nodes:,}, CITES={post_cites:,}, TEMP_CITES={post_temp}")
    assert post_nodes == 34546, f"Expected 34,546 nodes, got {post_nodes}"
    assert post_cites == 421578, f"Expected 421,578 CITES relationships, got {post_cites}"
    assert post_temp == 0, f"Expected 0 TEMP_CITES relationships, got {post_temp}"

    q7_stats = compute_latency_stats(q7_latencies_ms)
    q7_stats["qps"] = round(TOTAL_Q7_OPERATIONS / q7_wall_time, 2)
    q7_stats["total_wall_sec"] = round(q7_wall_time, 3)
    q7_stats["errors"] = errors
    q7_stats["canonical_cites_preserved"] = (pre_cites == 421578 and post_cites == 421578)
    q7_stats["temp_cites_remaining"] = post_temp

    print("\n--- Apache AGE Q7 Results ---")
    print(f"Total Operations: {TOTAL_Q7_OPERATIONS} (Reads: {read_count}, Writes: {write_count})")
    print(f"Workers:          {CONCURRENCY_WORKERS}")
    print(f"QPS:              {q7_stats['qps']}")
    print(f"Total Wall Time:  {q7_stats['total_wall_sec']} s")
    print(f"p50 Latency:      {q7_stats['p50_ms']} ms")
    print(f"p95 Latency:      {q7_stats['p95_ms']} ms")
    print(f"Min Latency:      {q7_stats['min_ms']} ms")
    print(f"Max Latency:      {q7_stats['max_ms']} ms")
    print(f"Mean Latency:     {q7_stats['mean_ms']} ms")
    print(f"StdDev Latency:   {q7_stats['stddev_ms']} ms")
    print(f"Errors:           {q7_stats['errors']}")
    print(f"Canonical CITES:  {post_cites:,} (Preserved: {q7_stats['canonical_cites_preserved']})")
    print(f"Residual TEMP:    {post_temp}")

    # Step 4: Update ONLY Apache AGE Q7 in benchmark_results.json
    if errors == 0:
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["databases"]["Apache AGE"]["Q7"] = q7_stats

        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"\nSuccessfully updated ONLY Apache AGE Q7 in {RESULTS_FILE}")
    else:
        print(f"\nErrors occurred ({errors}). benchmark_results.json was NOT updated.")


if __name__ == "__main__":
    run_age_q7_standalone()
