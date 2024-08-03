from dataclasses import dataclass
from pathlib import Path


@dataclass
class DEFAULTS:
    TOD_CANDIDATES_CSV_PATH = Path("tod_candidates.csv")
    TOD_CHECK_CSV_PATH = Path("tod_check.csv")
    TOD_MINER_STATS_PATH = Path("mining_stats.json")
    TRACES_PATH = Path("traces")
    RESULTS_PATH = Path("results")
    TIMINGS_PATH = Path("timings.csv")
    STATS_PATH = Path("stats")
    TOD_METHOD = "adapted"
