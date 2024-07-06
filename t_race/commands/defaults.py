from dataclasses import dataclass
from pathlib import Path


@dataclass
class DEFAULTS:
    TOD_CANDIDATES_CSV_PATH = Path("mined_tods.csv")
    TOD_MINER_STATS_PATH = Path("mining_stats.json")
    TRACES_PATH = Path("traces")
    RESULTS_PATH = Path("results")
    TIMINGS_PATH = Path("timings.csv")
