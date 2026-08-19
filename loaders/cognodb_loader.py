import os
import sys
import time
from typing import Dict, Any
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Add workspace root to sys.path to allow importing common
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.dataset import get_unique_nodes, stream_edge_batches, DEFAULT_DATASET_PATH

load_dotenv()

COGNODB_URI = os.getenv("COGNODB_URI")
COGNODB_USERNAME = os.getenv("COGNODB_USERNAME", "cognodb")
COGNODB_PASSWORD = os.getenv("COGNODB_PASSWORD")

if not COGNODB_URI or not COGNODB_PASSWORD:
    raise ValueError("COGNODB_URI and COGNODB_PASSWORD must be configured in your .env file.")

NODE_BATCH_SIZE = 5000
EDGE_BATCH_SIZE = 5000


def measure_network_latency(driver, samples: int = 5) -> float:
    """
    Measures the baseline client-to-cloud round-trip network latency over TLS.
    """
    latencies = []
    with driver.session() as session:
        for _ in range(samples):
            t0 = time.perf_counter()
            res = session.run("RETURN 1 AS ping")
            res.consume()
            latencies.append((time.perf_counter() - t0) * 1000.0)
    avg_latency = sum(latencies) / len(latencies)
    return round(avg_latency, 2)


def clear_and_prepare_cognodb(driver) -> None:
    """
    Clears all existing data in safe transaction chunks, creates
    the uniqueness constraint on :Paper(id), and verifies it is active.
    """
    with driver.session() as session:
        print("Clearing existing CognoDB graph data...")
        while True:
            res = session.run(
                "MATCH ()-[r:CITES]->() WITH r LIMIT 20000 DELETE r RETURN count(r) AS c"
            )
            deleted = res.single()["c"]
            if deleted == 0:
                break

        while True:
            res = session.run(
                "MATCH (n:Paper) WITH n LIMIT 20000 DELETE n RETURN count(n) AS c"
            )
            deleted = res.single()["c"]
            if deleted == 0:
                break

        # Fallback catch-all
        while True:
            res = session.run(
                "MATCH (n) WITH n LIMIT 5000 DETACH DELETE n RETURN count(n) AS c"
            )
            deleted = res.single()["c"]
            if deleted == 0:
                break

        print("Creating uniqueness constraint on :Paper(id)...")
        res = session.run(
            "CREATE CONSTRAINT paper_id_unique IF NOT EXISTS "
            "FOR (p:Paper) REQUIRE p.id IS UNIQUE"
        )
        res.consume()

        # Explicit verification of active constraint / index
        print("Verifying active constraints and indexes in CognoDB...")
        constraints = session.run("SHOW CONSTRAINTS").data()
        indexes = session.run("SHOW INDEXES").data()

        constraint_active = any(
            c.get("label") == "Paper" and "id" in str(c.get("properties", ""))
            for c in constraints
        )
        index_active = any(
            idx.get("label") == "Paper" and "id" in str(idx.get("properties", ""))
            for idx in indexes
        )

        if constraint_active:
            print(f"Verified active uniqueness constraint on :Paper(id): {constraints}")
        elif index_active:
            print(f"Verified active index on :Paper(id): {indexes}")
        else:
            raise RuntimeError(
                f"Uniqueness constraint on :Paper(id) was not found in CognoDB schema! "
                f"Constraints: {constraints}, Indexes: {indexes}"
            )


def load_cognodb_dataset(
    uri: str = COGNODB_URI,
    username: str = COGNODB_USERNAME,
    password: str = COGNODB_PASSWORD,
    dataset_path: str = DEFAULT_DATASET_PATH,
) -> Dict[str, Any]:
    """
    Two-phase bulk ingestion for CognoDB Cloud over Bolt/TLS:
    1. Bulk create unique Paper nodes using UNWIND and constraint.
    2. Bulk create CITES relationships using UNWIND and indexed MATCH.
    """
    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    print(f"Connected to CognoDB Cloud at {uri}!")

    # Measure baseline network ping
    avg_rtt_ms = measure_network_latency(driver, samples=5)
    print(f"Client-to-Cloud Baseline RTT Latency: {avg_rtt_ms:.2f} ms")

    clear_and_prepare_cognodb(driver)

    # ---------------- Phase 1: Ingest Nodes ----------------
    print("\n--- Phase 1: Extracting and Loading Unique Nodes ---")
    node_extract_start = time.perf_counter()
    unique_nodes = get_unique_nodes(dataset_path)
    node_extract_elapsed = time.perf_counter() - node_extract_start
    total_nodes = len(unique_nodes)
    print(f"Extracted {total_nodes:,} unique nodes in {node_extract_elapsed:.2f}s.")

    node_load_start = time.perf_counter()
    nodes_loaded = 0

    with driver.session() as session:
        for i in range(0, total_nodes, NODE_BATCH_SIZE):
            batch = unique_nodes[i : i + NODE_BATCH_SIZE]
            result = session.run(
                """
                UNWIND $batch AS nodeId
                CREATE (:Paper {id: nodeId})
                """,
                batch=batch,
            )
            result.consume()
            nodes_loaded += len(batch)
            if nodes_loaded % 10000 == 0 or nodes_loaded == total_nodes:
                print(f"  Nodes loaded: {nodes_loaded:,} / {total_nodes:,}")

    node_load_elapsed = time.perf_counter() - node_load_start
    nodes_per_sec = nodes_loaded / node_load_elapsed if node_load_elapsed > 0 else 0
    print(
        f"Phase 1 Complete: {nodes_loaded:,} nodes loaded in {node_load_elapsed:.2f}s "
        f"({nodes_per_sec:,.2f} nodes/sec)"
    )

    # ---------------- Phase 2: Ingest Relationships ----------------
    print("\n--- Phase 2: Streaming and Loading CITES Relationships ---")
    rel_load_start = time.perf_counter()
    edges_loaded = 0

    with driver.session() as session:
        for batch in stream_edge_batches(dataset_path, batch_size=EDGE_BATCH_SIZE):
            result = session.run(
                """
                UNWIND $batch AS edge
                MATCH (s:Paper {id: edge.source})
                MATCH (t:Paper {id: edge.target})
                CREATE (s)-[:CITES]->(t)
                """,
                batch=batch,
            )
            result.consume()
            edges_loaded += len(batch)
            if edges_loaded % 50000 == 0:
                print(f"  Relationships loaded: {edges_loaded:,}...")

    rel_load_elapsed = time.perf_counter() - rel_load_start
    edges_per_sec = edges_loaded / rel_load_elapsed if rel_load_elapsed > 0 else 0
    total_load_elapsed = node_load_elapsed + rel_load_elapsed

    print(
        f"Phase 2 Complete: {edges_loaded:,} relationships loaded in {rel_load_elapsed:.2f}s "
        f"({edges_per_sec:,.2f} rels/sec)"
    )

    # ---------------- Phase 3: Verification ----------------
    print("\n--- Phase 3: Verification ---")
    with driver.session() as session:
        verified_nodes = session.run(
            "MATCH (p:Paper) RETURN count(p) AS count"
        ).single()["count"]
        verified_edges = session.run(
            "MATCH ()-[r:CITES]->() RETURN count(r) AS count"
        ).single()["count"]

    driver.close()

    status = "SUCCESS" if (verified_nodes == 34546 and verified_edges == 421578) else "FAILED"

    results = {
        "status": status,
        "nodes_loaded": nodes_loaded,
        "verified_nodes": verified_nodes,
        "node_load_time_sec": round(node_load_elapsed, 3),
        "nodes_per_second": round(nodes_per_sec, 2),
        "relationships_loaded": edges_loaded,
        "verified_relationships": verified_edges,
        "relationship_load_time_sec": round(rel_load_elapsed, 3),
        "relationships_per_second": round(edges_per_sec, 2),
        "total_wall_clock_sec": round(total_load_elapsed, 3),
        "baseline_rtt_ms": avg_rtt_ms,
    }

    print("\n" + "=" * 50)
    print(f"COGNODB CLOUD INGESTION SUMMARY [{status}]")
    print("=" * 50)
    print(f"Nodes Verified:        {verified_nodes:,} / 34,546")
    print(f"Edges Verified:        {verified_edges:,} / 421,578")
    print(f"Node Load Time:        {results['node_load_time_sec']} s ({results['nodes_per_second']:,} nodes/s)")
    print(f"Edge Load Time:        {results['relationship_load_time_sec']} s ({results['relationships_per_second']:,} rels/s)")
    print(f"Total Wall-Clock Time: {results['total_wall_clock_sec']} s")
    print(f"Baseline Network RTT:  {results['baseline_rtt_ms']} ms")
    print("=" * 50)

    return results


if __name__ == "__main__":
    load_cognodb_dataset()
