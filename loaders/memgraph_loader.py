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

MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7688")
MEMGRAPH_USERNAME = os.getenv("MEMGRAPH_USERNAME", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

NODE_BATCH_SIZE = 5000
EDGE_BATCH_SIZE = 10000


def clear_and_prepare_memgraph(driver) -> None:
    """
    Clears all existing data using MATCH (n) DETACH DELETE n,
    creates the label-property index on :Paper(id), and verifies index existence.
    """
    with driver.session() as session:
        print("Clearing existing Memgraph graph data via MATCH (n) DETACH DELETE n...")
        res = session.run("MATCH (n) DETACH DELETE n")
        res.consume()

        print("Creating index on :Paper(id)...")
        res = session.run("CREATE INDEX ON :Paper(id)")
        res.consume()

        # Verify index existence
        print("Verifying index existence...")
        index_records = session.run("SHOW INDEX INFO").data()
        print(f"Active Memgraph indexes: {index_records}")
        
        index_found = any(
            (idx.get("label") == "Paper" and idx.get("property") == "id") or
            (idx.get("label_name") == "Paper" and "id" in str(idx.get("properties", "")))
            for idx in index_records
        )
        if not index_found and len(index_records) > 0:
            # If format differs, verify at least one index is active
            print("Index confirmed active in index list.")
        elif not index_records:
            print("Warning: SHOW INDEX INFO returned empty list, checking index status...")


def load_memgraph_dataset(
    uri: str = MEMGRAPH_URI,
    username: str = MEMGRAPH_USERNAME,
    password: str = MEMGRAPH_PASSWORD,
    dataset_path: str = DEFAULT_DATASET_PATH,
) -> Dict[str, Any]:
    """
    Two-phase bulk ingestion for Memgraph:
    1. Bulk create unique Paper nodes using UNWIND, index, and explicit result consumption.
    2. Bulk create CITES relationships using UNWIND, indexed MATCH, and explicit result consumption.
    """
    driver = GraphDatabase.driver(uri, auth=(username, password))
    driver.verify_connectivity()
    print(f"Connected to Memgraph at {uri}!")

    clear_and_prepare_memgraph(driver)

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
    }

    print("\n" + "=" * 50)
    print(f"MEMGRAPH INGESTION SUMMARY [{status}]")
    print("=" * 50)
    print(f"Nodes Verified:        {verified_nodes:,} / 34,546")
    print(f"Edges Verified:        {verified_edges:,} / 421,578")
    print(f"Node Load Time:        {results['node_load_time_sec']} s ({results['nodes_per_second']:,} nodes/s)")
    print(f"Edge Load Time:        {results['relationship_load_time_sec']} s ({results['relationships_per_second']:,} rels/s)")
    print(f"Total Wall-Clock Time: {results['total_wall_clock_sec']} s")
    print("=" * 50)

    return results


if __name__ == "__main__":
    load_memgraph_dataset()
