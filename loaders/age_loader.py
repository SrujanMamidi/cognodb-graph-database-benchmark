import os
import sys
import time
import json
from typing import Dict, Any
from dotenv import load_dotenv
import psycopg2

# Add workspace root to sys.path to allow importing common
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.dataset import get_unique_nodes, stream_edge_batches, DEFAULT_DATASET_PATH

load_dotenv()

AGE_HOST = os.getenv("AGE_HOST", "localhost")
AGE_PORT = int(os.getenv("AGE_PORT", "5432"))
AGE_USER = os.getenv("AGE_USER", "postgres")
AGE_PASSWORD = os.getenv("AGE_PASSWORD", "AgeBench2026!")
AGE_DBNAME = os.getenv("AGE_DBNAME", "postgres")
AGE_GRAPH = os.getenv("AGE_GRAPH", "benchmark")

NODE_BATCH_SIZE = 5000
EDGE_BATCH_SIZE = 5000


def get_age_connection():
    """
    Establishes connection to PostgreSQL and initializes the AGE environment.
    """
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


def clear_and_prepare_age(conn, graph_name: str = AGE_GRAPH) -> None:
    """
    Resets the graph 'benchmark', pre-creates labels 'Paper' and 'CITES',
    and builds a GIN index on Paper(properties) for fast matching.
    """
    with conn.cursor() as cur:
        print(f"Resetting Apache AGE graph '{graph_name}'...")
        cur.execute("SELECT 1 FROM ag_graph WHERE name = %s;", (graph_name,))
        if cur.fetchone():
            cur.execute(f"SELECT drop_graph('{graph_name}', true);")

        cur.execute(f"SELECT create_graph('{graph_name}');")
        cur.execute(f"SELECT create_vlabel('{graph_name}', 'Paper');")
        cur.execute(f"SELECT create_elabel('{graph_name}', 'CITES');")

        print("Creating GIN index on Paper(properties)...")
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS paper_props_gin ON {graph_name}."Paper" USING gin (properties);'
        )

        # Prepare server-side parameterized queries to avoid large SQL string building
        print("Preparing server-side Cypher batch execution statements...")
        cur.execute(f"""
            PREPARE age_insert_nodes(agtype) AS 
            SELECT * FROM cypher('{graph_name}', $$ 
                UNWIND $batch AS nodeId 
                CREATE (:Paper {{id: nodeId}}) 
            $$, $1) AS (a agtype);
        """)

        cur.execute(f"""
            PREPARE age_insert_edges(agtype) AS 
            SELECT * FROM cypher('{graph_name}', $$ 
                UNWIND $batch AS edge 
                MATCH (s:Paper {{id: edge.source}}) 
                MATCH (t:Paper {{id: edge.target}}) 
                CREATE (s)-[:CITES]->(t) 
            $$, $1) AS (a agtype);
        """)


def load_age_dataset(
    graph_name: str = AGE_GRAPH,
    dataset_path: str = DEFAULT_DATASET_PATH,
) -> Dict[str, Any]:
    """
    Two-phase bulk ingestion for Apache AGE using parameterized prepared statements:
    1. Bulk create unique Paper nodes using parameterized Cypher UNWIND.
    2. Bulk create CITES relationships using indexed MATCH and CREATE.
    """
    conn = get_age_connection()
    print(f"Connected to Apache AGE at {AGE_HOST}:{AGE_PORT}, graph '{graph_name}'!")

    clear_and_prepare_age(conn, graph_name)

    # ---------------- Phase 1: Ingest Nodes ----------------
    print("\n--- Phase 1: Extracting and Loading Unique Nodes ---")
    node_extract_start = time.perf_counter()
    unique_nodes = get_unique_nodes(dataset_path)
    node_extract_elapsed = time.perf_counter() - node_extract_start
    total_nodes = len(unique_nodes)
    print(f"Extracted {total_nodes:,} unique nodes in {node_extract_elapsed:.2f}s.")

    node_load_start = time.perf_counter()
    nodes_loaded = 0

    with conn.cursor() as cur:
        for i in range(0, total_nodes, NODE_BATCH_SIZE):
            batch = unique_nodes[i : i + NODE_BATCH_SIZE]
            param_json = json.dumps({"batch": batch})
            cur.execute("EXECUTE age_insert_nodes(%s::agtype);", (param_json,))
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

    with conn.cursor() as cur:
        for batch in stream_edge_batches(dataset_path, batch_size=EDGE_BATCH_SIZE):
            param_json = json.dumps({"batch": batch})
            cur.execute("EXECUTE age_insert_edges(%s::agtype);", (param_json,))
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
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM cypher('{graph_name}', $$ MATCH (p:Paper) RETURN count(p) $$) AS (count agtype);"
        )
        raw_nodes = cur.fetchone()[0]
        verified_nodes = int(str(raw_nodes).strip('"'))

        cur.execute(
            f"SELECT * FROM cypher('{graph_name}', $$ MATCH ()-[r:CITES]->() RETURN count(r) $$) AS (count agtype);"
        )
        raw_edges = cur.fetchone()[0]
        verified_edges = int(str(raw_edges).strip('"'))

    conn.close()

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
    print(f"APACHE AGE INGESTION SUMMARY [{status}]")
    print("=" * 50)
    print(f"Nodes Verified:        {verified_nodes:,} / 34,546")
    print(f"Edges Verified:        {verified_edges:,} / 421,578")
    print(f"Node Load Time:        {results['node_load_time_sec']} s ({results['nodes_per_second']:,} nodes/s)")
    print(f"Edge Load Time:        {results['relationship_load_time_sec']} s ({results['relationships_per_second']:,} rels/s)")
    print(f"Total Wall-Clock Time: {results['total_wall_clock_sec']} s")
    print("=" * 50)

    return results


if __name__ == "__main__":
    load_age_dataset()
