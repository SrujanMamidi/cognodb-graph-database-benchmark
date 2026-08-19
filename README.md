# Empirical Graph Database Benchmark: Stanford SNAP cit-HepPh

An empirical, reproducible, and unbiased performance benchmark evaluating five graph database engines on the Stanford SNAP High Energy Physics Phenomenology citation network (`cit-HepPh`).

---

## 1. Project Overview and Objective

This project provides a reproducible benchmark evaluating five graph database systems on standard graph workloads under resource-constrained execution environments:

1. **Neo4j Community Edition**
2. **Memgraph Community Edition**
3. **FalkorDB**
4. **Apache AGE (PostgreSQL Extension)**
5. **CognoDB Cloud**

The benchmark measures:
* **Bulk Ingestion Throughput:** Two-phase batch insertion rates for nodes and relationships.
* **Point & Filtered Lookups:** Index seeks on unique integer identifiers.
* **Variable-Depth Graph Traversals:** 1-hop, 2-hop, and 3-hop traversal latency distributions.
* **Full-Graph Analytical Aggregation:** Out-degree distribution grouping and ranking across all edges.
* **Concurrent Multi-Client Mixed Workloads:** Multi-threaded throughput (QPS), transaction latency, and conflict resilience under concurrent reads (80%) and writes (20%).

---

## 2. Database Platforms Tested

| Database Engine | Engine Architecture | Query Interface | Deployment Model | Protocol / Port |
| :--- | :--- | :--- | :--- | :--- |
| **Neo4j Community** | Native Graph Engine (Disk-backed, JVM) | Cypher 25 | Docker container (`neo4j-benchmark`) | Bolt (`7687`) |
| **Memgraph Community** | In-Memory Graph Engine (C++) | openCypher | Docker container (`memgraph-benchmark`) | Bolt (`7688`) |
| **FalkorDB** | Sparse Matrix Graph Engine (GraphBLAS) | Cypher | Docker container (`falkordb-benchmark`) | RESP / Redis (`6379`) |
| **Apache AGE** | Relational Table Graph Extension (PostgreSQL 18) | openCypher via SQL | Docker container (`age-benchmark`) | PostgreSQL (`5432`) |
| **CognoDB Cloud** | Native Cloud Graph Database | Cypher 5 | Managed Cloud (`us-east4`) | Bolt TLS (`bolt+s://`) |

---

## 3. Dataset Source and Canonical Model

The benchmark utilizes the real-world citation network **`cit-HepPh`** (High Energy Physics Phenomenology) from the Stanford Network Analysis Platform (SNAP).

* **Source File:** `data/cit-HepPh.txt.gz`
* **Graph Type:** Directed, unweighted citation graph
* **Nodes:** **34,546** unique papers (`(:Paper {id: integer})`)
* **Relationships:** **421,578** directed citations (`(:Paper)-[:CITES]->(:Paper)`)
* **Average Out-Degree:** ~12.2 citations per paper
* **Canonical Logical Model:**
  ```text
  (:Paper {id: <integer>})-[:CITES]->(:Paper {id: <integer>})
  ```

No synthetic nodes, placeholder properties, or mock entities were introduced.

---

## 4. Hardware and Resource Configuration

### Resource Configuration Matrix

| Target System | Configured CPU Limit | Configured RAM Limit | Memory Swap Limit | Observable Live Memory Footprint | Storage Layer |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Neo4j Community** | **0.5 vCPU** (`500,000,000 nCPUs`) | **512 MB** (`536,870,912 B`) | 1,024 MB | ~501.8 MiB | Container Volume (`/data`) |
| **Memgraph Community** | **0.5 vCPU** (`500,000,000 nCPUs`) | **512 MB** (`536,870,912 B`) | 1,024 MB | ~137.6 MiB | In-Memory Engine |
| **FalkorDB** | **0.5 vCPU** (`500,000,000 nCPUs`) | **512 MB** (`536,870,912 B`) | 1,024 MB | ~231.1 MiB | In-Memory (GraphBLAS) |
| **Apache AGE** | **0.5 vCPU** (`500,000,000 nCPUs`) | **512 MB** (`536,870,912 B`) | 1,024 MB | ~190.0 MiB | PostgreSQL 18 Relational Tables |
| **CognoDB Cloud** | **~0.5 vCPU** (advertised burst) | **512 MB** (advertised) | Managed | *Not observable* | Cloud Managed (1 GiB quota) |

* **Host Environment:** Windows 11 Host, Python 3.12 (`uv` virtual environment).
* **Docker Resource Enforcement:** Implemented via Linux cgroups (`docker update --cpus 0.5 --memory 512m --memory-swap 1024m`).
* **CognoDB Cloud Baseline Network RTT:** Measured over 20 TLS ping samples (`RETURN 1`):
  * **p50:** `270.864 ms` | **p95:** `326.035 ms` | **Min / Max:** `269.281 ms / 487.832 ms` | **Mean / StdDev:** `284.208 ms / 47.818 ms`

---

## 5. Fairness Methodology & Resource Limitations

### Methodological Resource Statement
> **"Neo4j, Memgraph, FalkorDB, and Apache AGE were constrained to a common 0.5 vCPU / 512 MB RAM Docker resource profile. CognoDB Cloud used its advertised managed cloud profile; its underlying server resources are not independently observable."**

### Key Fairness Considerations
1. **Resource Boundary Limitation:** The local Docker containers were restricted to identical CPU quota (`50,000 / 100,000 µs`) and 512 MB memory envelopes via cgroups. CognoDB Cloud is a managed cloud instance whose host CPU clock speed, hypervisor scheduling, and physical memory bus are managed by the cloud provider and cannot be directly probed or configured.
2. **Network Transport Difference:** Local databases were queried over localhost loopback sockets (sub-millisecond RTT), whereas CognoDB Cloud was accessed over TLS 1.3 across the public Internet to GCP `us-east4` (~270.86 ms baseline RTT). Client-measured latencies for CognoDB Cloud naturally reflect transport overhead on micro-queries.
3. **No Synthetic Normalization:** All reported measurements represent raw, unadjusted wall-clock timings captured with high-resolution performance timers (`time.perf_counter()`).

---

## 6. Data Loading Methodology

Each database was loaded from an empty baseline using dedicated loaders in `loaders/`:

* **Phase 1 (Nodes):** 34,546 unique integer IDs were extracted deterministically from `data/cit-HepPh.txt.gz` and ingested in batches of 5,000 nodes using `UNWIND`. An index/uniqueness constraint on `:Paper(id)` was created and verified active before proceeding to Phase 2.
* **Phase 2 (Relationships):** 421,578 directed edges were streamed in batches of 5,000 / 10,000 pairs using `UNWIND` and indexed node lookups (`MATCH (s:Paper {id: edge.source}), (t:Paper {id: edge.target}) CREATE (s)-[:CITES]->(t)`).
* **Verification:** Driver buffers were consumed (`result.consume()`), transactions committed, and final node/edge counts verified (`34,546` nodes, `421,578` relationships).

---

## 7. Benchmark Methodology

* **Deterministic Node Sampling:** A shared random sample of **100 node IDs** was generated using `seed=42` from the 34,546 valid nodes. All 5 databases executed queries against the exact same 100 node IDs in identical sequence.
* **Warm-Up Phase:** Exactly **20 unmeasured warm-up iterations** preceded every read workload to populate buffer pools, compile query plans, and stabilize execution caches.
* **Measurement Phase:** Exactly **100 measured iterations** were timed per read workload.
* **Percentile Calculation:** Latencies were sorted to calculate exact **p50 (median)**, **p95 (95th percentile)**, **min**, **max**, **mean**, and **standard deviation**.
* **Isolated Concurrency:** Q7 executed 1,000 concurrent operations across 20 worker threads using ephemeral `[:TEMP_CITES]` edges followed by a mandatory cleanup routine.

---

## 8. Workload Definitions (Q1–Q7)

| Workload ID | Name | Logical Cypher Query | Description |
| :--- | :--- | :--- | :--- |
| **Q1** | **1-Hop Traversal** | `MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m)` | Counts direct citation out-neighbors. |
| **Q2** | **2-Hop Traversal** | `MATCH (p:Paper {id: $id})-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m)` | 2-step citation path expansion ($P \rightarrow A \rightarrow B$). |
| **Q3** | **3-Hop Deep Traversal** | `MATCH (p:Paper {id: $id})-[:CITES]->()-[:CITES]->()-[:CITES]->(m:Paper) RETURN count(m)` | Deep 3-step graph traversal ($P \rightarrow A \rightarrow B \rightarrow C$). |
| **Q4** | **Point Lookup** | `MATCH (p:Paper {id: $id}) RETURN p.id AS id` | Retrieves a single paper by primary ID. |
| **Q5** | **Filtered / Indexed Lookup** | `MATCH (p:Paper) WHERE p.id = $id RETURN p.id AS id` | Filtered property lookup leveraging the `Paper.id` index. |
| **Q6** | **Analytical Aggregation** | `MATCH (p:Paper)-[:CITES]->(m:Paper) RETURN p.id AS paper, count(m) AS out_degree ORDER BY out_degree DESC LIMIT 10` | Full-graph out-degree aggregation and top-10 ranking. |
| **Q7** | **Concurrent Mixed Workload** | **80% Reads (Q1):** `MATCH (p:Paper {id: $id})-[:CITES]->(m:Paper) RETURN count(m)`<br>**20% Writes:** `MATCH (s:Paper {id: $src}), (t:Paper {id: $tgt}) CREATE (s)-[:TEMP_CITES {worker: $wid, ts: $ts}]->(t)` | 20 concurrent threads executing 1,000 total operations with post-run cleanup. |

---

## 9. Full Ingestion Results

| Database Platform | Verified Nodes | Verified Edges | Node Load Time (s) | Node Rate (nodes/s) | Edge Load Time (s) | Edge Rate (edges/s) | Total Wall Time (s) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Memgraph Community** | **34,546** | **421,578** | **0.691** | 49,990.97 | **21.710** | **19,418.93** | **22.401** |
| **CognoDB Cloud** | **34,546** | **421,578** | 2.709 | 12,752.81 | **62.955** | 6,696.45 | **65.664** |
| **FalkorDB** | **34,546** | **421,578** | **0.513** | **67,349.93** | 69.980 | 6,024.29 | **70.493** |
| **Neo4j Community** | **34,546** | **421,578** | 9.852 | 3,506.43 | 93.173 | 4,524.69 | **103.025** |
| **Apache AGE** | **34,546** | **421,578** | 1.332 | 25,936.26 | 737.580 | 571.57 | **738.912** |

---

## 10. Full Q1–Q6 Read Workload Results Matrix

All latencies are reported in **milliseconds (ms)** from `results/benchmark_results.json`.

### Master Latency Matrix (p50 / p95)

| Workload ID | Neo4j (p50 / p95) | Memgraph (p50 / p95) | FalkorDB (p50 / p95) | Apache AGE (p50 / p95) | CognoDB Cloud* (p50 / p95) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Q1: 1-Hop Traversal** | 14.003 / 100.658 ms | 4.841 / 33.397 ms | **3.992 / 7.283 ms** | 3.952 / 11.178 ms | 270.707 / 283.104 ms |
| **Q2: 2-Hop Traversal** | 11.576 / 98.045 ms | **3.784** / 13.190 ms | 4.100 / **8.291 ms** | 6.359 / 51.819 ms | 272.525 / 297.775 ms |
| **Q3: 3-Hop Deep Traversal** | 7.485 / 98.273 ms | 4.613 / 31.982 ms | **4.098 / 6.431 ms** | 186.947 / 399.265 ms | 273.162 / 311.824 ms |
| **Q4: Point Lookup by ID** | 7.367 / 96.521 ms | 3.585 / 19.407 ms | 3.139 / **4.680 ms** | **2.390** / 9.302 ms | 270.821 / 288.617 ms |
| **Q5: Filtered / Indexed Lookup** | 5.466 / 74.931 ms | 4.972 / 43.078 ms | **3.280 / 5.233 ms** | 81.891 / 99.397 ms | 271.807 / 295.018 ms |
| **Q6: Analytical Aggregation** | 2,688.301 / 3,297.857 ms | 990.490 / 1,268.978 ms | **891.745 / 1,199.686 ms** | 4,289.275 / 5,816.970 ms | 2,400.919 / 2,602.393 ms |

*\*CognoDB Cloud measured latencies include ~270.86 ms baseline TLS client-to-cloud round-trip transport time.*

### Detailed Latency Distributions (ms)

| Database | Workload | Min (ms) | Max (ms) | Mean (ms) | StdDev (ms) | p50 (ms) | p95 (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Neo4j** | **Q1** | 5.987 | 1,288.901 | 62.128 | 131.295 | 14.003 | 100.658 |
| | **Q2** | 5.112 | 220.840 | 42.007 | 43.156 | 11.576 | 98.045 |
| | **Q3** | 4.058 | 793.553 | 39.856 | 87.331 | 7.485 | 98.273 |
| | **Q4** | 4.595 | 5,232.088 | 94.057 | 518.095 | 7.367 | 96.521 |
| | **Q5** | 4.389 | 86.512 | 16.041 | 23.429 | 5.466 | 74.931 |
| | **Q6** | 1,894.415 | 4,899.646 | 2,622.543 | 563.699 | 2,688.301 | 3,297.857 |
| **Memgraph** | **Q1** | 2.814 | 62.434 | 8.696 | 11.426 | 4.841 | 33.397 |
| | **Q2** | 2.586 | 31.596 | 5.451 | 4.895 | 3.784 | 13.190 |
| | **Q3** | 2.608 | 58.449 | 8.354 | 10.128 | 4.613 | 31.982 |
| | **Q4** | 2.307 | 30.587 | 6.049 | 5.670 | 3.585 | 19.407 |
| | **Q5** | 2.714 | 67.340 | 11.826 | 13.663 | 4.972 | 43.078 |
| | **Q6** | 820.999 | 1,603.619 | 1,021.826 | 132.037 | 990.490 | 1,268.978 |
| **FalkorDB** | **Q1** | 2.902 | 29.339 | 4.675 | 2.836 | 3.992 | 7.283 |
| | **Q2** | 2.698 | 15.370 | 4.533 | 1.775 | 4.100 | 8.291 |
| | **Q3** | 2.677 | 13.672 | 4.477 | 1.421 | 4.098 | 6.431 |
| | **Q4** | 2.399 | 7.178 | 3.331 | 0.771 | 3.139 | 4.680 |
| | **Q5** | 2.442 | 7.660 | 3.508 | 0.869 | 3.280 | 5.233 |
| | **Q6** | 701.902 | 1,475.965 | 919.942 | 153.146 | 891.745 | 1,199.686 |
| **Apache AGE** | **Q1** | 2.488 | 177.191 | 7.737 | 19.271 | 3.952 | 11.178 |
| | **Q2** | 3.850 | 208.901 | 14.405 | 24.307 | 6.359 | 51.819 |
| | **Q3** | 95.097 | 1,302.419 | 194.164 | 144.829 | 186.947 | 399.265 |
| | **Q4** | 1.715 | 19.350 | 3.210 | 2.593 | 2.390 | 9.302 |
| | **Q5** | 30.947 | 109.806 | 71.987 | 23.745 | 81.891 | 99.397 |
| | **Q6** | 3,713.863 | 7,402.378 | 4,469.079 | 671.688 | 4,289.275 | 5,816.970 |
| **CognoDB Cloud** | **Q1** | 269.140 | 344.414 | 273.913 | 11.025 | 270.707 | 283.104 |
| | **Q2** | 269.234 | 397.436 | 277.369 | 15.272 | 272.525 | 297.775 |
| | **Q3** | 269.525 | 329.426 | 278.311 | 13.051 | 273.162 | 311.824 |
| | **Q4** | 269.423 | 525.555 | 275.348 | 25.824 | 270.821 | 288.617 |
| | **Q5** | 269.129 | 344.051 | 276.389 | 12.228 | 271.807 | 295.018 |
| | **Q6** | 2,247.353 | 2,797.411 | 2,430.032 | 99.849 | 2,400.919 | 2,602.393 |

---

## 11. Q7 Concurrent Mixed Workload Results

* **Parameters:** 20 concurrent workers, 1,000 total operations (800 Reads / 200 Writes).
* **Cleanup Protocol:** Mandatory execution of `MATCH ()-[r:TEMP_CITES]->() DELETE r` followed by strict count assertions.

| Database Platform | Concurrent Workers | Throughput (QPS) | Total Wall Time (s) | p50 Latency (ms) | p95 Latency (ms) | Min (ms) | Max (ms) | Mean (ms) | StdDev (ms) | Errors | CITES Preserved (421,578) | Residual TEMP_CITES |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Memgraph Community** | **20** | **133.67** | **7.481** | **109.180** | **316.352** | 3.182 | 1,038.309 | 146.518 | 111.684 | **0** | **Yes (421,578)** | **0** |
| **CognoDB Cloud** | **20** | **66.03** | **15.145** | 265.865 | **430.219** | 245.368 | 1,437.986 | 299.743 | 162.027 | **0** | **Yes (421,578)** | **0** |
| **FalkorDB** | **20** | **65.00** | **15.384** | **55.909** | 282.934 | 2.825 | 501.131 | 77.633 | 84.556 | **0** | **Yes (421,578)** | **0** |
| **Neo4j Community** | **20** | **40.87** | **24.470** | 362.312 | 1,307.673 | 9.294 | 2,403.135 | 483.969 | 388.496 | **0** | **Yes (421,578)** | **0** |
| **Apache AGE** | **20** | **11.86** | **84.300** | 492.203 | 1,113.951 | 8.118 | 3,206.279 | 536.464 | 367.666 | **0** | **Yes (421,578)** | **0** |

---

## 12. Query Plan & Index Verification

Index utilization on `MATCH (p:Paper) WHERE p.id = $id RETURN p.id` was verified via execution plans:

| Database Engine | Target Index | Index Type | Observable Execution Plan / Optimizer Evidence |
| :--- | :--- | :--- | :--- |
| **Neo4j Community** | `Paper(id)` | Uniqueness Constraint | `NodeUniqueIndexSeek` on `UNIQUE p:Paper(id) WHERE id = $id` |
| **Memgraph Community** | `Paper(id)` | Label+Property Index | `ScanAllByLabelProperties (p :Paper {id})` |
| **FalkorDB** | `Paper(id)` | Label Property Index | `Node By Index Scan \| (p:Paper)` |
| **Apache AGE** | `Paper(properties)` | PostgreSQL GIN Index | Filter: `(agtype_access_operator(VARIADIC ARRAY[properties, '"id"'::agtype]) = '1000'::agtype)` |
| **CognoDB Cloud** | `Paper(id)` | Uniqueness Constraint | `NodeIndexSeek` on `:Paper(id)` |

---

## 13. Data Integrity Verification

| Database Platform | Expected Nodes | Verified Nodes | Expected CITES | Verified CITES | Residual TEMP_CITES | Integrity Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Neo4j Community** | 34,546 | **34,546** | 421,578 | **421,578** | **0** | **PASS** |
| **Memgraph Community** | 34,546 | **34,546** | 421,578 | **421,578** | **0** | **PASS** |
| **FalkorDB** | 34,546 | **34,546** | 421,578 | **421,578** | **0** | **PASS** |
| **Apache AGE** | 34,546 | **34,546** | 421,578 | **421,578** | **0** | **PASS** |
| **CognoDB Cloud** | 34,546 | **34,546** | 421,578 | **421,578** | **0** | **PASS** |

---

## 14. Performance Analysis & Key Findings

1. **In-Memory Graph Traversal Dominance:** **Memgraph** and **FalkorDB** demonstrated lowest latency profiles on traversals (Q1–Q3 p50: ~3.7 ms–4.8 ms). FalkorDB's GraphBLAS matrix representation yielded the lowest tail latency (Q3 p95: 6.431 ms) and fastest analytical aggregation (Q6 p50: 891.745 ms).
2. **Deep Traversal Scaling in Relational Hybrid Engines:** While **Apache AGE** provided very fast point lookups (Q4 p50: 2.390 ms), multi-hop traversal latencies escalated on 3-hop paths (Q3 p50: 186.947 ms) due to recursive SQL relational join overhead.
3. **Garbage Collection and Memory Constraints:** **Neo4j** operated stably inside the strict 512 MB cgroup envelope with a 128 MB heap, but experienced higher tail variance under memory pressure (Q1 p95: 100.658 ms, Q4 max: 5,232.088 ms).
4. **Cloud Database Performance Profile:** **CognoDB Cloud** demonstrated consistent server-side execution (Q6 aggregation completed in 2,400.919 ms, outperforming Neo4j and Apache AGE). Single-hop queries reflect the ~270.86 ms TLS transit to `us-east4`. Under concurrent load (Q7), its connection multiplexer sustained 66.03 QPS with 0 errors.

---

## 15. Project Structure

```text
cognodb-benchmark/
├── README.md                  # Comprehensive benchmark report & reproduction guide
├── pyproject.toml             # Python environment and dependency specifications
├── .env.example               # Environment variable templates (no credentials)
├── .gitignore                 # Excludes .env, secrets, venv, caches, and scratch files
├── data/
│   └── cit-HepPh.txt.gz       # Canonical Stanford SNAP cit-HepPh citation dataset
├── common/
│   ├── __init__.py
│   ├── dataset.py             # Deterministic gzip parsing and streaming batch generator
│   └── seed.py                # Deterministic seed generator (seed=42, 100 node IDs)
├── loaders/
│   ├── __init__.py
│   ├── neo4j_loader.py        # Neo4j two-phase bulk loader and constraint manager
│   ├── memgraph_loader.py     # Memgraph two-phase bulk loader and index manager
│   ├── falkordb_loader.py     # FalkorDB two-phase bulk loader and index manager
│   ├── age_loader.py          # Apache AGE two-phase bulk loader and GIN index manager
│   └── cognodb_loader.py      # CognoDB Cloud two-phase bulk loader and schema manager
├── benchmarks/
│   ├── __init__.py
│   ├── config.py              # Shared parameters (20 warmups, 100 iterations, 20 workers)
│   ├── metrics.py             # Statistical calculations (p50, p95, min, max, mean, stddev)
│   ├── smoke_test.py          # Pre-benchmark sanity verification across all 5 engines
│   ├── run_all.py             # Master benchmark orchestrator
│   └── runners/
│       ├── __init__.py
│       ├── neo4j_runner.py    # Neo4j Q1-Q7 workload runner
│       ├── memgraph_runner.py # Memgraph Q1-Q7 workload runner
│       ├── falkordb_runner.py # FalkorDB Q1-Q7 workload runner
│       ├── age_runner.py      # Apache AGE Q1-Q7 workload runner
│       └── cognodb_runner.py  # CognoDB Cloud Q1-Q7 workload runner
└── results/
    └── benchmark_results.json # Full raw benchmark JSON metrics artifact
```

---

## 16. Reproducibility & Execution Instructions

### Prerequisites
* Python 3.12+ and `uv` package manager installed.
* Docker Desktop running.

### Step 1: Environment Setup
Clone the repository and install dependencies:
```bash
uv sync
```

Copy the environment template and populate your database credentials:
```bash
cp .env.example .env
```

### Step 2: Launch Docker Containers with 0.5 CPU / 512 MB RAM Constraints
```bash
# Launch containers
docker run -d --name neo4j-benchmark --cpus="0.5" --memory="512m" -p 7687:7687 -e NEO4J_AUTH=neo4j/Neo4jBench2026! neo4j:latest
docker run -d --name memgraph-benchmark --cpus="0.5" --memory="512m" -p 7688:7687 memgraph/memgraph:latest
docker run -d --name falkordb-benchmark --cpus="0.5" --memory="512m" -p 6379:6379 falkordb/falkordb:latest
docker run -d --name age-benchmark --cpus="0.5" --memory="512m" -p 5432:5432 -e POSTGRES_PASSWORD=AgeBench2026! apache/age:latest
```

### Step 3: Run Ingestion Loaders
Ingest the canonical dataset (34,546 nodes, 421,578 relationships) into each database:
```bash
uv run python loaders/neo4j_loader.py
uv run python loaders/memgraph_loader.py
uv run python loaders/falkordb_loader.py
uv run python loaders/age_loader.py
uv run python loaders/cognodb_loader.py
```

### Step 4: Execute Smoke Tests
Verify connectivity and query parity across all 5 engines:
```bash
uv run python benchmarks/smoke_test.py
```

### Step 5: Execute Full Benchmark Suite
Execute the master benchmark across all 5 databases and output results to `results/benchmark_results.json`:
```bash
uv run python benchmarks/run_all.py
```

---

## 17. Final Conclusion

This benchmark provides a rigorous, empirical baseline comparing five graph database systems on the Stanford SNAP `cit-HepPh` dataset:

* **Memgraph Community:** Delivered the highest ingestion throughput (19,418 edges/s) and highest concurrent transactional throughput (133.67 QPS).
* **FalkorDB:** Produced the lowest tail latency across deep multi-hop traversals (Q3 p95: 6.431 ms) and the fastest full-graph analytical aggregation (Q6 p50: 891.745 ms).
* **Neo4j Community:** Maintained standard Cypher compliance and transactional integrity within a strict 512 MB memory boundary.
* **Apache AGE:** Offered strong relational synergy and fast single-record lookups (Q4 p50: 2.390 ms), with trade-offs during recursive joins and write contention.
* **CognoDB Cloud:** Demonstrated solid cloud-native graph analytical capability and multi-client concurrency (66.03 QPS), with client-measured response times reflecting cloud TLS transport latency.

All raw metrics are preserved in [`results/benchmark_results.json`](file:///c:/Users/Srujan%20Mamidi/Desktop/cognodb-benchmark/results/benchmark_results.json).
