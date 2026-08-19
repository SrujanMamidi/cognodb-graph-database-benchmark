import os
import sys
import time
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from neo4j import GraphDatabase

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.seed import get_shared_test_nodes, get_random_node_pairs
from benchmarks.config import WARMUP_ITERATIONS, MEASURED_ITERATIONS, CONCURRENCY_WORKERS, TOTAL_Q7_OPERATIONS, READ_RATIO
from benchmarks.metrics import compute_latency_stats

load_dotenv()

COGNODB_URI = os.getenv("COGNODB_URI")
COGNODB_USERNAME = os.getenv("COGNODB_USERNAME", "cognodb")
COGNODB_PASSWORD = os.getenv("COGNODB_PASSWORD")

QUERIES = {
    "Q1": "MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m) AS count",
    "Q2": "MATCH (p:Paper {id: $id})-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m) AS count",
    "Q3": "MATCH (p:Paper {id: $id})-[:CITES]->()-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m) AS count",
    "Q4": "MATCH (p:Paper {id: $id}) RETURN p.id AS id",
    "Q5": "MATCH (p:Paper) WHERE p.id = $id RETURN p.id AS id",
    "Q6": "MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 10",
}


def get_driver():
    return GraphDatabase.driver(COGNODB_URI, auth=(COGNODB_USERNAME, COGNODB_PASSWORD))


def get_cognodb_query_plan(query_key: str = "Q5") -> Dict[str, Any]:
    driver = get_driver()
    plan_str = ""
    try:
        with driver.session() as session:
            # Query plan inspection
            res = session.run(f"EXPLAIN {QUERIES[query_key]}", id=1000)
            summary = res.consume()
            if summary.plan:
                plan_str = str(summary.plan)
            else:
                plan_str = "not observable (server-side query plan not returned in Bolt summary)"
    except Exception as e:
        plan_str = f"not observable ({e})"
    finally:
        driver.close()
    return {
        "indexed_property": "Paper(id)",
        "index_type": "Uniqueness constraint (paper_id_unique)",
        "query_plan_evidence": plan_str,
    }


def measure_cognodb_rtt(driver, samples: int = 20) -> Dict[str, float]:
    latencies = []
    with driver.session() as session:
        for _ in range(samples):
            t0 = time.perf_counter()
            res = session.run("RETURN 1 AS ping")
            res.consume()
            latencies.append((time.perf_counter() - t0) * 1000.0)
    return compute_latency_stats(latencies)


def run_cognodb_benchmark() -> Dict[str, Any]:
    driver = get_driver()
    driver.verify_connectivity()
    test_nodes = get_shared_test_nodes()
    results = {}

    # Network baseline
    print("Measuring CognoDB Cloud baseline TLS round-trip latency...")
    results["baseline_rtt"] = measure_cognodb_rtt(driver, samples=20)

    # Verify initial canonical count
    with driver.session() as session:
        initial_cites = session.run("MATCH ()-[r:CITES]->() RETURN count(r) AS c").single()["c"]

    # ---------------- Read Workloads Q1 to Q6 ----------------
    for q_name in ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]:
        query = QUERIES[q_name]
        print(f"Running CognoDB Cloud {q_name} (20 warm-up, 100 measured)...")

        # Warm-up phase
        with driver.session() as session:
            for i in range(WARMUP_ITERATIONS):
                node_id = test_nodes[i % len(test_nodes)]
                if q_name == "Q6":
                    res = session.run(query)
                else:
                    res = session.run(query, id=node_id)
                res.consume()

        # Measurement phase
        latencies_ms = []
        with driver.session() as session:
            for i in range(MEASURED_ITERATIONS):
                node_id = test_nodes[i % len(test_nodes)]
                t0 = time.perf_counter()
                if q_name == "Q6":
                    res = session.run(query)
                else:
                    res = session.run(query, id=node_id)
                res.consume()
                latencies_ms.append((time.perf_counter() - t0) * 1000.0)

        results[q_name] = compute_latency_stats(latencies_ms)

    # ---------------- Concurrent Mixed Workload Q7 ----------------
    print(f"Running CognoDB Cloud Q7 (Concurrent Mixed: {CONCURRENCY_WORKERS} workers, {TOTAL_Q7_OPERATIONS} ops, 80/20 ratio)...")
    node_pairs = get_random_node_pairs(count=TOTAL_Q7_OPERATIONS)
    q7_latencies_ms = []
    errors = 0

    def worker_task(task_id: int):
        src, tgt = node_pairs[task_id % len(node_pairs)]
        is_write = (task_id % 100) >= int(READ_RATIO * 100)
        with driver.session() as session:
            t0 = time.perf_counter()
            try:
                if is_write:
                    res = session.run(
                        "MATCH (s:Paper {id: $src}), (t:Paper {id: $tgt}) "
                        "CREATE (s)-[:TEMP_CITES {worker: $wid, ts: $ts}]->(t)",
                        src=src,
                        tgt=tgt,
                        wid=task_id,
                        ts=time.time(),
                    )
                    res.consume()
                else:
                    res = session.run(QUERIES["Q1"], id=src)
                    res.consume()
                return (time.perf_counter() - t0) * 1000.0, None
            except Exception as e:
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
    with driver.session() as session:
        del_res = session.run("MATCH ()-[r:TEMP_CITES]->() DELETE r")
        del_res.consume()
        final_cites = session.run("MATCH ()-[r:CITES]->() RETURN count(r) AS c").single()["c"]
        remaining_temp = session.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]

    q7_stats = compute_latency_stats(q7_latencies_ms)
    q7_stats["qps"] = round(TOTAL_Q7_OPERATIONS / q7_wall_time, 2)
    q7_stats["total_wall_sec"] = round(q7_wall_time, 3)
    q7_stats["errors"] = errors
    q7_stats["canonical_cites_preserved"] = (initial_cites == 421578 and final_cites == 421578)
    q7_stats["temp_cites_remaining"] = remaining_temp
    results["Q7"] = q7_stats

    # Query plan info
    results["Q5_plan"] = get_cognodb_query_plan("Q5")
    driver.close()
    return results


if __name__ == "__main__":
    import json
    res = run_cognodb_benchmark()
    print(json.dumps(res, indent=2))
