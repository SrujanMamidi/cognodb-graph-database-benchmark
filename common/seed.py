import random
from typing import List, Tuple
from common.dataset import get_unique_nodes, DEFAULT_DATASET_PATH

SHARED_RANDOM_SEED = 42
SAMPLE_NODE_COUNT = 100


def get_shared_test_nodes(
    dataset_path: str = DEFAULT_DATASET_PATH,
    sample_size: int = SAMPLE_NODE_COUNT,
    seed: int = SHARED_RANDOM_SEED,
) -> List[int]:
    """
    Returns a deterministic, reproducible sample of node IDs from the dataset.
    All databases will execute read workloads against this exact list.
    """
    nodes = get_unique_nodes(dataset_path)
    rng = random.Random(seed)
    return rng.sample(nodes, sample_size)


def get_random_node_pairs(
    dataset_path: str = DEFAULT_DATASET_PATH,
    count: int = 1000,
    seed: int = SHARED_RANDOM_SEED + 1,
) -> List[Tuple[int, int]]:
    """
    Generates deterministic random node pairs for Q7 mixed concurrent workload.
    """
    nodes = get_unique_nodes(dataset_path)
    rng = random.Random(seed)
    pairs = []
    for _ in range(count):
        src = rng.choice(nodes)
        tgt = rng.choice(nodes)
        pairs.append((src, tgt))
    return pairs
