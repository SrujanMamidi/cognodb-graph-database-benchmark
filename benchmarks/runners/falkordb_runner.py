import os
import sys
import time
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from falkordb import FalkorDB

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.seed import get_shared_test_nodes, get_random_node_pairs
from benchmarks.config import WARMUP_ITERATIONS, MEASURED_ITERATIONS, CONCURRENCY_WORKERS, TOTAL_Q7_OPERATIONS, READ_RATIO
from benchmarks.metrics import compute_latency_stats

load_dotenv()

FALKORDB_HOST = os.getenv("FALKORDB_HOST", "localhost")
FALKORDB_PORT = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "benchmark")

QUERIES = {
    "Q1": "MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m) AS count",
    "Q2": "MATCH (p:Paper {id: $id})-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m) AS count",
    "Q3": "MATCH (p:Paper {id: $id})-[:CITES]->()-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m) AS count",
    "Q4": "MATCH (p:Paper {id: $id}) RETURN p.id AS id",
    "Q5": "MATCH (p:Paper) WHERE p.id = $id RETURN p.id AS id",
    "Q6": "MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 10",
}


def get_graph():
    db = FalkorDB(host=FALKORDB_HOST, port=FALKORDB_PORT)
    return db.select_graph(FALKORDB_GRAPH)


def get_falkordb_query_plan(query_key: str = "Q5") -> Dict[str, Any]:
    graph = get_graph()
    plan_str = ""
    try:
        res = graph.execute_command("GRAPH.EXPLAIN", FALKORDB_GRAPH, "MATCH (p:Paper) WHERE p.id = 1000 RETURN p.id AS id")
        plan_str = "\n".join(str(line) for line in res)
    except Exception as e:
        plan_str = f"Error capturing plan: {e}"
    return {
        "indexed_property": "Paper(id)",
        "index_type": "Label index (Paper.id)",
        "query_plan_evidence": plan_str,
    }


def run_falkordb_benchmark() -> Dict[str, Any]:
    graph = get_graph()
    test_nodes = get_shared_test_nodes()
    results = {}

    # Verify initial canonical count
    initial_cites = graph.query("MATCH ()-[r:CITES]->() RETURN count(r) AS c", timeout=30000).result_set[0][0]

    # ---------------- Read Workloads Q1 to Q6 ----------------
    for q_name in ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]:
        print(f"Running FalkorDB {q_name} (20 warm-up, 100 measured)...")

        # Warm-up phase
        for i in range(WARMUP_ITERATIONS):
            node_id = test_nodes[i % len(test_nodes)]
            if q_name == "Q6":
                graph.query(QUERIES[q_name], timeout=30000)
            elif q_name in ["Q1", "Q2", "Q3", "Q4"]:
                q_str = QUERIES[q_name].replace("$id", str(node_id))
                graph.query(q_str, timeout=30000)
            elif q_name == "Q5":
                graph.query(f"MATCH (p:Paper) WHERE p.id = {node_id} RETURN p.id AS id", timeout=30000)

        # Measurement phase
        latencies_ms = []
        for i in range(MEASURED_ITERATIONS):
            node_id = test_nodes[i % len(test_nodes)]
            t0 = time.perf_counter()
            if q_name == "Q6":
                graph.query(QUERIES[q_name], timeout=30000)
            elif q_name in ["Q1", "Q2", "Q3", "Q4"]:
                q_str = QUERIES[q_name].replace("$id", str(node_id))
                graph.query(q_str, timeout=30000)
            elif q_name == "Q5":
                graph.query(f"MATCH (p:Paper) WHERE p.id = {node_id} RETURN p.id AS id", timeout=30000)
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)

        results[q_name] = compute_latency_stats(latencies_ms)

    # ---------------- Concurrent Mixed Workload Q7 ----------------
    print(f"Running FalkorDB Q7 (Concurrent Mixed: {CONCURRENCY_WORKERS} workers, {TOTAL_Q7_OPERATIONS} ops, 80/20 ratio)...")
    node_pairs = get_random_node_pairs(count=TOTAL_Q7_OPERATIONS)
    q7_latencies_ms = []
    errors = 0

    def worker_task(task_id: int):
        src, tgt = node_pairs[task_id % len(node_pairs)]
        is_write = (task_id % 100) >= int(READ_RATIO * 100)
        local_graph = get_graph()
        t0 = time.perf_counter()
        try:
            if is_write:
                local_graph.query(
                    f"MATCH (s:Paper {{id: {src}}}), (t:Paper {{id: {tgt}}}) "
                    f"CREATE (s)-[:TEMP_CITES {{worker: {task_id}, ts: {time.time()}}}]->(t)",
                    timeout=30000,
                )
            else:
                local_graph.query(
                    f"MATCH (p:Paper {{id: {src}}})-[:CITES]->(m:Paper) RETURN count(m) AS count",
                    timeout=30000,
                )
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
    graph.query("MATCH ()-[r:TEMP_CITES]->() DELETE r", timeout=30000)
    final_cites = graph.query("MATCH ()-[r:CITES]->() RETURN count(r) AS c", timeout=30000).result_set[0][0]
    remaining_temp = graph.query("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c", timeout=30000).result_set[0][0]

    q7_stats = compute_latency_stats(q7_latencies_ms)
    q7_stats["qps"] = round(TOTAL_Q7_OPERATIONS / q7_wall_time, 2)
    q7_stats["total_wall_sec"] = round(q7_wall_time, 3)
    q7_stats["errors"] = errors
    q7_stats["canonical_cites_preserved"] = (initial_cites == 421578 and final_cites == 421578)
    q7_stats["temp_cites_remaining"] = remaining_temp
    results["Q7"] = q7_stats

    # Query plan info
    results["Q5_plan"] = get_falkordb_query_plan("Q5")
    return results


if __name__ == "__main__":
    import json
    res = run_falkordb_benchmark()
    print(json.dumps(res, indent=2))
