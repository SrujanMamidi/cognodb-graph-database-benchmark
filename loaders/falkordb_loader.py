import os
import sys
import time
from typing import Dict, Any
from dotenv import load_dotenv
from falkordb import FalkorDB

# Add workspace root to sys.path to allow importing common
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.dataset import get_unique_nodes, stream_edge_batches, DEFAULT_DATASET_PATH

load_dotenv()

FALKORDB_HOST = os.getenv("FALKORDB_HOST", "localhost")
FALKORDB_PORT = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_GRAPH = os.getenv("FALKORDB_GRAPH", "benchmark")

NODE_BATCH_SIZE = 5000
EDGE_BATCH_SIZE = 5000


def clear_and_prepare_falkordb(graph) -> None:
    """
    Clears existing FalkorDB graph data and creates the index on :Paper(id).
    """
    print(f"Clearing existing FalkorDB graph '{graph.name}'...")
    try:
        graph.delete()
    except Exception:
        # Fallback if graph did not exist yet
        graph.query("MATCH (n) DETACH DELETE n")

    print("Creating index on :Paper(id)...")
    graph.query("CREATE INDEX FOR (p:Paper) ON (p.id)")


def load_falkordb_dataset(
    host: str = FALKORDB_HOST,
    port: int = FALKORDB_PORT,
    graph_name: str = FALKORDB_GRAPH,
    dataset_path: str = DEFAULT_DATASET_PATH,
) -> Dict[str, Any]:
    """
    Two-phase bulk ingestion for FalkorDB:
    1. Bulk create unique Paper nodes using UNWIND and index.
    2. Bulk create CITES relationships using UNWIND and indexed MATCH.
    """
    db = FalkorDB(host=host, port=port)
    graph = db.select_graph(graph_name)
    print(f"Connected to FalkorDB at {host}:{port}, graph '{graph_name}'!")

    clear_and_prepare_falkordb(graph)

    # ---------------- Phase 1: Ingest Nodes ----------------
    print("\n--- Phase 1: Extracting and Loading Unique Nodes ---")
    node_extract_start = time.perf_counter()
    unique_nodes = get_unique_nodes(dataset_path)
    node_extract_elapsed = time.perf_counter() - node_extract_start
    total_nodes = len(unique_nodes)
    print(f"Extracted {total_nodes:,} unique nodes in {node_extract_elapsed:.2f}s.")

    node_load_start = time.perf_counter()
    nodes_loaded = 0

    for i in range(0, total_nodes, NODE_BATCH_SIZE):
        batch = unique_nodes[i : i + NODE_BATCH_SIZE]
        graph.query(f"UNWIND {batch} AS nodeId CREATE (:Paper {{id: nodeId}})")
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

    for batch in stream_edge_batches(dataset_path, batch_size=EDGE_BATCH_SIZE):
        edge_pairs = [[row["source"], row["target"]] for row in batch]
        graph.query(
            f"UNWIND {edge_pairs} AS edge "
            "MATCH (s:Paper {id: edge[0]}) "
            "MATCH (t:Paper {id: edge[1]}) "
            "CREATE (s)-[:CITES]->(t)"
        )
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
    res_nodes = graph.query("MATCH (p:Paper) RETURN count(p) AS count")
    verified_nodes = res_nodes.result_set[0][0]

    res_edges = graph.query("MATCH ()-[r:CITES]->() RETURN count(r) AS count")
    verified_edges = res_edges.result_set[0][0]

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
    print(f"FALKORDB INGESTION SUMMARY [{status}]")
    print("=" * 50)
    print(f"Nodes Verified:        {verified_nodes:,} / 34,546")
    print(f"Edges Verified:        {verified_edges:,} / 421,578")
    print(f"Node Load Time:        {results['node_load_time_sec']} s ({results['nodes_per_second']:,} nodes/s)")
    print(f"Edge Load Time:        {results['relationship_load_time_sec']} s ({results['relationships_per_second']:,} rels/s)")
    print(f"Total Wall-Clock Time: {results['total_wall_clock_sec']} s")
    print("=" * 50)

    return results


if __name__ == "__main__":
    load_falkordb_dataset()
