import os
import sys
import time
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import psycopg2

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.seed import get_shared_test_nodes, get_random_node_pairs
from benchmarks.config import WARMUP_ITERATIONS, MEASURED_ITERATIONS, CONCURRENCY_WORKERS, TOTAL_Q7_OPERATIONS, READ_RATIO
from benchmarks.metrics import compute_latency_stats

load_dotenv()

AGE_HOST = os.getenv("AGE_HOST", "localhost")
AGE_PORT = int(os.getenv("AGE_PORT", "5432"))
AGE_USER = os.getenv("AGE_USER", "postgres")
AGE_PASSWORD = os.getenv("AGE_PASSWORD")
AGE_DBNAME = os.getenv("AGE_DBNAME", "postgres")
AGE_GRAPH = os.getenv("AGE_GRAPH", "benchmark")

if not AGE_PASSWORD:
    raise ValueError("AGE_PASSWORD must be configured in your .env file.")


def get_age_connection():
    conn = psycopg2.connect(
        host=AGE_HOST,
        port=AGE_PORT,
        user=AGE_USER,
        password=AGE_PASSWORD,
        dbname=AGE_DBNAME,
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("LOAD 'age';")
        cur.execute('SET search_path = ag_catalog, "$user", public;')
    return conn


def get_age_query_plan(query_key: str = "Q5") -> Dict[str, Any]:
    conn = get_age_connection()
    plan_lines = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"EXPLAIN ANALYZE SELECT * FROM cypher('{AGE_GRAPH}', $$ "
                "MATCH (p:Paper) WHERE p.id = 1000 RETURN p.id AS id "
                "$$) AS (id agtype);"
            )
            for row in cur.fetchall():
                plan_lines.append(row[0])
    except Exception as e:
        plan_lines.append(f"Error capturing plan: {e}")
    finally:
        conn.close()
    return {
        "indexed_property": "Paper(properties)",
        "index_type": "GIN index (paper_props_gin)",
        "query_plan_evidence": "\n".join(plan_lines),
    }


def run_age_benchmark() -> Dict[str, Any]:
    conn = get_age_connection()
    test_nodes = get_shared_test_nodes()
    results = {}

    # Verify initial canonical count
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:CITES]->() RETURN count(r) $$) AS (c agtype);")
        initial_cites = int(str(cur.fetchone()[0]).strip('"'))

    # ---------------- Read Workloads Q1 to Q6 ----------------
    for q_name in ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]:
        print(f"Running Apache AGE {q_name} (20 warm-up, 100 measured)...")

        def build_sql(node_id: int) -> str:
            if q_name == "Q1":
                return f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper {{id: {node_id}}})-[:CITES]->(m:Paper) RETURN count(m) $$) AS (c agtype);"
            elif q_name == "Q2":
                return f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper {{id: {node_id}}})-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m) $$) AS (c agtype);"
            elif q_name == "Q3":
                return f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper {{id: {node_id}}})-[:CITES]->()-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m) $$) AS (c agtype);"
            elif q_name == "Q4":
                return f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper {{id: {node_id}}}) RETURN p.id AS id $$) AS (id agtype);"
            elif q_name == "Q5":
                return f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper) WHERE p.id = {node_id} RETURN p.id AS id $$) AS (id agtype);"
            elif q_name == "Q6":
                return f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 10 $$) AS (paper agtype, out_degree agtype);"
            return ""

        # Warm-up phase
        with conn.cursor() as cur:
            for i in range(WARMUP_ITERATIONS):
                node_id = test_nodes[i % len(test_nodes)]
                cur.execute(build_sql(node_id))
                cur.fetchall()

        # Measurement phase
        latencies_ms = []
        with conn.cursor() as cur:
            for i in range(MEASURED_ITERATIONS):
                node_id = test_nodes[i % len(test_nodes)]
                t0 = time.perf_counter()
                cur.execute(build_sql(node_id))
                cur.fetchall()
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)

        results[q_name] = compute_latency_stats(latencies_ms)

    # ---------------- Concurrent Mixed Workload Q7 ----------------
    print(f"Running Apache AGE Q7 (Concurrent Mixed: {CONCURRENCY_WORKERS} workers, {TOTAL_Q7_OPERATIONS} ops, 80/20 ratio)...")
    node_pairs = get_random_node_pairs(count=TOTAL_Q7_OPERATIONS)
    q7_latencies_ms = []
    errors = 0

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

    # Mandatory Cleanup of TEMP_CITES
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:TEMP_CITES]->() DELETE r $$) AS (a agtype);")
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:CITES]->() RETURN count(r) $$) AS (c agtype);")
        final_cites = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute(f"SELECT * FROM cypher('{AGE_GRAPH}', $$ MATCH ()-[r:TEMP_CITES]->() RETURN count(r) $$) AS (c agtype);")
        remaining_temp = int(str(cur.fetchone()[0]).strip('"'))

    q7_stats = compute_latency_stats(q7_latencies_ms)
    q7_stats["qps"] = round(TOTAL_Q7_OPERATIONS / q7_wall_time, 2)
    q7_stats["total_wall_sec"] = round(q7_wall_time, 3)
    q7_stats["errors"] = errors
    q7_stats["canonical_cites_preserved"] = (initial_cites == 421578 and final_cites == 421578)
    q7_stats["temp_cites_remaining"] = remaining_temp
    results["Q7"] = q7_stats

    # Query plan info
    results["Q5_plan"] = get_age_query_plan("Q5")
    conn.close()
    return results


if __name__ == "__main__":
    import json
    res = run_age_benchmark()
    print(json.dumps(res, indent=2))
