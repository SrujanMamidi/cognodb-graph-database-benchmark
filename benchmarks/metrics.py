import math
from typing import List, Dict, Any


def compute_latency_stats(latencies_ms: List[float]) -> Dict[str, float]:
    """
    Computes statistical percentiles and summary metrics from a list of latencies in milliseconds.
    """
    if not latencies_ms:
        return {
            "count": 0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
            "mean_ms": 0.0,
            "stddev_ms": 0.0,
        }

    sorted_lats = sorted(latencies_ms)
    n = len(sorted_lats)

    def percentile(p: float) -> float:
        k = (n - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_lats[int(k)]
        d0 = sorted_lats[int(f)] * (c - k)
        d1 = sorted_lats[int(c)] * (k - f)
        return d0 + d1

    mean_val = sum(sorted_lats) / n
    variance = sum((x - mean_val) ** 2 for x in sorted_lats) / n if n > 1 else 0.0
    stddev_val = math.sqrt(variance)

    return {
        "count": n,
        "p50_ms": round(percentile(50.0), 3),
        "p95_ms": round(percentile(95.0), 3),
        "min_ms": round(min(sorted_lats), 3),
        "max_ms": round(max(sorted_lats), 3),
        "mean_ms": round(mean_val, 3),
        "stddev_ms": round(stddev_val, 3),
    }
