import os
import sys
import time
import json
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.seed import get_shared_test_nodes, get_random_node_pairs
from benchmarks.runners.neo4j_runner import get_driver as get_neo4j_driver, get_neo4j_query_plan
from benchmarks.runners.memgraph_runner import get_driver as get_memgraph_driver, get_memgraph_query_plan
from benchmarks.runners.falkordb_runner import get_graph as get_falkordb_graph, get_falkordb_query_plan
from benchmarks.runners.age_runner import get_age_connection, get_age_query_plan
from benchmarks.runners.cognodb_runner import get_driver as get_cognodb_driver, get_cognodb_query_plan

load_dotenv()


def smoke_test_neo4j() -> Dict[str, Any]:
    print("=== SMOKE TEST: Neo4j ===")
    driver = get_neo4j_driver()
    node = get_shared_test_nodes()[0]
    out = {}
    with driver.session() as s:
        # Q1
        r = s.run("MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m) AS c", id=node)
        out["Q1_result"] = r.single()["c"]
        # Q4
        r = s.run("MATCH (p:Paper {id: $id}) RETURN p.id AS id", id=node)
        out["Q4_result"] = r.single()["id"]
        # Q5
        r = s.run("MATCH (p:Paper) WHERE p.id = $id RETURN p.id AS id", id=node)
        out["Q5_result"] = r.single()["id"]
        # Q6
        r = s.run("MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 3")
        out["Q6_top3"] = [dict(rec) for rec in r]
        # Mini-Q7
        s.run("MATCH (s:Paper {id: $id}) CREATE (s)-[:TEMP_CITES {test: true}]->(s)", id=node).consume()
        temp_cnt = s.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]
        s.run("MATCH ()-[r:TEMP_CITES]->() DELETE r").consume()
        temp_after = s.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]
        cites_cnt = s.run("MATCH ()-[r:CITES]->() RETURN count(r) AS c").single()["c"]
        out["mini_q7_cleanup_verified"] = (temp_cnt == 1 and temp_after == 0 and cites_cnt == 421578)

    out["Q5_plan"] = get_neo4j_query_plan("Q5")
    driver.close()
    return out


def smoke_test_memgraph() -> Dict[str, Any]:
    print("=== SMOKE TEST: Memgraph ===")
    driver = get_memgraph_driver()
    node = get_shared_test_nodes()[0]
    out = {}
    with driver.session() as s:
        # Q1
        r = s.run("MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m) AS c", id=node)
        out["Q1_result"] = r.single()["c"]
        # Q4
        r = s.run("MATCH (p:Paper {id: $id}) RETURN p.id AS id", id=node)
        out["Q4_result"] = r.single()["id"]
        # Q5
        r = s.run("MATCH (p:Paper) WHERE p.id = $id RETURN p.id AS id", id=node)
        out["Q5_result"] = r.single()["id"]
        # Q6
        r = s.run("MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 3")
        out["Q6_top3"] = [dict(rec) for rec in r]
        # Mini-Q7
        s.run("MATCH (s:Paper {id: $id}) CREATE (s)-[:TEMP_CITES {test: true}]->(s)", id=node).consume()
        temp_cnt = s.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]
        s.run("MATCH ()-[r:TEMP_CITES]->() DELETE r").consume()
        temp_after = s.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]
        cites_cnt = s.run("MATCH ()-[r:CITES]->() RETURN count(r) AS c").single()["c"]
        out["mini_q7_cleanup_verified"] = (temp_cnt == 1 and temp_after == 0 and cites_cnt == 421578)

    out["Q5_plan"] = get_memgraph_query_plan("Q5")
    driver.close()
    return out


def smoke_test_falkordb() -> Dict[str, Any]:
    print("=== SMOKE TEST: FalkorDB ===")
    g = get_falkordb_graph()
    node = get_shared_test_nodes()[0]
    out = {}
    # Q1
    r = g.query(f"MATCH (p:Paper {{id: {node}}})-[:CITES]->(m:Paper) RETURN count(m) AS c", timeout=30000)
    out["Q1_result"] = r.result_set[0][0]
    # Q4
    r = g.query(f"MATCH (p:Paper {{id: {node}}}) RETURN p.id AS id", timeout=30000)
    out["Q4_result"] = r.result_set[0][0]
    # Q5
    r = g.query(f"MATCH (p:Paper) WHERE p.id = {node} RETURN p.id AS id", timeout=30000)
    out["Q5_result"] = r.result_set[0][0]
    # Q6
    r = g.query("MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 3", timeout=30000)
    out["Q6_top3"] = [{"paper": row[0], "out_degree": row[1]} for row in r.result_set]
    # Mini-Q7
    g.query(f"MATCH (s:Paper {{id: {node}}}) CREATE (s)-[:TEMP_CITES {{test: true}}]->(s)", timeout=30000)
    temp_cnt = g.query("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c", timeout=30000).result_set[0][0]
    g.query("MATCH ()-[r:TEMP_CITES]->() DELETE r", timeout=30000)
    temp_after = g.query("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c", timeout=30000).result_set[0][0]
    cites_cnt = g.query("MATCH ()-[r:CITES]->() RETURN count(r) AS c", timeout=30000).result_set[0][0]
    out["mini_q7_cleanup_verified"] = (temp_cnt == 1 and temp_after == 0 and cites_cnt == 421578)

    out["Q5_plan"] = get_falkordb_query_plan("Q5")
    return out


def smoke_test_age() -> Dict[str, Any]:
    print("=== SMOKE TEST: Apache AGE ===")
    conn = get_age_connection()
    node = get_shared_test_nodes()[0]
    out = {}
    with conn.cursor() as cur:
        # Q1
        cur.execute(f"SELECT * FROM cypher('benchmark', $$ MATCH (p:Paper {{id: {node}}})-[:CITES]->(m:Paper) RETURN count(m) $$) AS (c agtype);")
        out["Q1_result"] = int(str(cur.fetchone()[0]).strip('"'))
        # Q4
        cur.execute(f"SELECT * FROM cypher('benchmark', $$ MATCH (p:Paper {{id: {node}}}) RETURN p.id $$) AS (id agtype);")
        out["Q4_result"] = int(str(cur.fetchone()[0]).strip('"'))
        # Q5
        cur.execute(f"SELECT * FROM cypher('benchmark', $$ MATCH (p:Paper) WHERE p.id = {node} RETURN p.id $$) AS (id agtype);")
        out["Q5_result"] = int(str(cur.fetchone()[0]).strip('"'))
        # Q6
        cur.execute(f"SELECT * FROM cypher('benchmark', $$ MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 3 $$) AS (paper agtype, out_degree agtype);")
        out["Q6_top3"] = [{"paper": row[0], "out_degree": row[1]} for row in cur.fetchall()]
        # Mini-Q7
        cur.execute(f"SELECT * FROM cypher('benchmark', $$ MATCH (s:Paper {{id: {node}}}) CREATE (s)-[:TEMP_CITES {{test: true}}]->(s) $$) AS (a agtype);")
        cur.execute("SELECT * FROM cypher('benchmark', $$ MATCH ()-[r:TEMP_CITES]->() RETURN count(r) $$) AS (c agtype);")
        temp_cnt = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute("SELECT * FROM cypher('benchmark', $$ MATCH ()-[r:TEMP_CITES]->() DELETE r $$) AS (a agtype);")
        cur.execute("SELECT * FROM cypher('benchmark', $$ MATCH ()-[r:TEMP_CITES]->() RETURN count(r) $$) AS (c agtype);")
        temp_after = int(str(cur.fetchone()[0]).strip('"'))
        cur.execute("SELECT * FROM cypher('benchmark', $$ MATCH ()-[r:CITES]->() RETURN count(r) $$) AS (c agtype);")
        cites_cnt = int(str(cur.fetchone()[0]).strip('"'))
        out["mini_q7_cleanup_verified"] = (temp_cnt == 1 and temp_after == 0 and cites_cnt == 421578)

    out["Q5_plan"] = get_age_query_plan("Q5")
    conn.close()
    return out


def smoke_test_cognodb() -> Dict[str, Any]:
    print("=== SMOKE TEST: CognoDB Cloud ===")
    driver = get_cognodb_driver()
    node = get_shared_test_nodes()[0]
    out = {}
    with driver.session() as s:
        # Q1
        r = s.run("MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m) AS c", id=node)
        out["Q1_result"] = r.single()["c"]
        # Q4
        r = s.run("MATCH (p:Paper {id: $id}) RETURN p.id AS id", id=node)
        out["Q4_result"] = r.single()["id"]
        # Q5
        r = s.run("MATCH (p:Paper) WHERE p.id = $id RETURN p.id AS id", id=node)
        out["Q5_result"] = r.single()["id"]
        # Q6
        r = s.run("MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 3")
        out["Q6_top3"] = [dict(rec) for rec in r]
        # Mini-Q7
        s.run("MATCH (s:Paper {id: $id}) CREATE (s)-[:TEMP_CITES {test: true}]->(s)", id=node).consume()
        temp_cnt = s.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]
        s.run("MATCH ()-[r:TEMP_CITES]->() DELETE r").consume()
        temp_after = s.run("MATCH ()-[r:TEMP_CITES]->() RETURN count(r) AS c").single()["c"]
        cites_cnt = s.run("MATCH ()-[r:CITES]->() RETURN count(r) AS c").single()["c"]
        out["mini_q7_cleanup_verified"] = (temp_cnt == 1 and temp_after == 0 and cites_cnt == 421578)

    out["Q5_plan"] = get_cognodb_query_plan("Q5")
    driver.close()
    return out


def run_all_smoke_tests():
    all_results = {}
    all_results["Neo4j"] = smoke_test_neo4j()
    all_results["Memgraph"] = smoke_test_memgraph()
    all_results["FalkorDB"] = smoke_test_falkordb()
    all_results["Apache AGE"] = smoke_test_age()
    all_results["CognoDB Cloud"] = smoke_test_cognodb()

    print("\n" + "=" * 60)
    print("ALL 5 SMOKE TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(json.dumps(all_results, indent=2))
    return all_results


if __name__ == "__main__":
    run_all_smoke_tests()
