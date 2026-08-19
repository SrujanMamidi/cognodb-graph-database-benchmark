import gzip
import os
from typing import Generator, List, Dict, Set

DEFAULT_DATASET_PATH = os.path.join("data", "cit-HepPh.txt.gz")


def get_unique_nodes(filepath: str = DEFAULT_DATASET_PATH) -> List[int]:
    """
    Extracts all unique node IDs from the dataset.
    Returns a sorted list of integer node IDs.
    """
    nodes: Set[int] = set()
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                nodes.add(int(parts[0]))
                nodes.add(int(parts[1]))
    return sorted(nodes)


def stream_edge_batches(
    filepath: str = DEFAULT_DATASET_PATH, batch_size: int = 10000
) -> Generator[List[Dict[str, int]], None, None]:
    """
    Streams edges from the compressed dataset in batches of specified size.
    Each edge is represented as a dictionary: {'source': int, 'target': int}.
    """
    batch: List[Dict[str, int]] = []
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                batch.append({"source": int(parts[0]), "target": int(parts[1])})
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
    if batch:
        yield batch


def get_dataset_stats(filepath: str = DEFAULT_DATASET_PATH) -> Dict[str, int]:
    """
    Returns total node count and relationship count in the dataset.
    """
    nodes: Set[int] = set()
    edge_count = 0
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                nodes.add(int(parts[0]))
                nodes.add(int(parts[1]))
                edge_count += 1
    return {
        "unique_nodes": len(nodes),
        "total_edges": edge_count,
    }
