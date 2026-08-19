import os

WARMUP_ITERATIONS = 20
MEASURED_ITERATIONS = 100

CONCURRENCY_WORKERS = 20
TOTAL_Q7_OPERATIONS = 1000
READ_RATIO = 0.8
WRITE_RATIO = 0.2

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
RESULTS_FILE = os.path.join(RESULTS_DIR, "benchmark_results.json")
