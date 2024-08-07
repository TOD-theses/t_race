from dataclasses import dataclass
from pathlib import Path


@dataclass
class DEFAULTS:
    TOD_CANDIDATES_CSV_PATH = Path("tod_candidates.csv")
    TOD_MINING_EVALUATION_CSV_PATH = Path("tod_candidates_evaluation.csv")
    TOD_CHECK_CSV_PATH = Path("tod_check.csv")
    TOD_CHECK_JSONL_PATH = Path("tod_check_details.jsonl")
    TOD_PROPERTIES_CSV_PATH = Path("tod_properties.csv")
    TOD_PROPERTIES_JSONL_PATH = Path("tod_properties_details.jsonl")
    TOD_MINER_STATS_PATH = Path("mining_stats.json")
    TIMINGS_PATH = Path("timings.csv")
    STATS_PATH = Path("stats")
    TOD_METHOD = "overall"
