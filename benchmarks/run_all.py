import os
import sys
import json
import time
from typing import Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from benchmarks.config import RESULTS_DIR, RESULTS_FILE
from benchmarks.runners.neo4j_runner import run_neo4j_benchmark
from benchmarks.runners.memgraph_runner import run_memgraph_benchmark
from benchmarks.runners.falkordb_runner import run_falkordb_benchmark
from benchmarks.runners.age_runner import run_age_benchmark
from benchmarks.runners.cognodb_runner import run_cognodb_benchmark


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    all_results: Dict[str, Any] = {
        "metadata": {
            "dataset": "Stanford SNAP cit-HepPh",
            "nodes": 34546,
            "relationships": 421578,
            "resource_configuration": {
                "neo4j": "0.5 vCPU / 512 MB RAM (Docker cgroups)",
                "memgraph": "0.5 vCPU / 512 MB RAM (Docker cgroups)",
                "falkordb": "0.5 vCPU / 512 MB RAM (Docker cgroups)",
                "apache_age": "0.5 vCPU / 512 MB RAM (Docker cgroups)",
                "cognodb_cloud": "~0.5 vCPU / 512 MB RAM (Configured Cloud Specification, us-east4)"
            },
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        },
        "databases": {}
    }

    print("\n" + "=" * 60)
    print("STARTING FULL BENCHMARK SUITE ACROSS ALL 5 DATABASES")
    print("=" * 60)

    # 1. Neo4j
    print("\n>>> BENCHMARKING NEO4J COMMUNITY <<<")
    all_results["databases"]["Neo4j"] = run_neo4j_benchmark()

    # 2. Memgraph
    print("\n>>> BENCHMARKING MEMGRAPH COMMUNITY <<<")
    all_results["databases"]["Memgraph"] = run_memgraph_benchmark()

    # 3. FalkorDB
    print("\n>>> BENCHMARKING FALKORDB <<<")
    all_results["databases"]["FalkorDB"] = run_falkordb_benchmark()

    # 4. Apache AGE
    print("\n>>> BENCHMARKING APACHE AGE <<<")
    all_results["databases"]["Apache AGE"] = run_age_benchmark()

    # 5. CognoDB Cloud
    print("\n>>> BENCHMARKING COGNODB CLOUD <<<")
    all_results["databases"]["CognoDB Cloud"] = run_cognodb_benchmark()

    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print("\n" + "=" * 60)
    print(f"BENCHMARK COMPLETE! Results written to: {RESULTS_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()
