from argparse import ArgumentParser, Namespace
import argparse
import csv
from dataclasses import dataclass
from importlib.metadata import version
import json
from pathlib import Path
from typing import Iterable

import psycopg
from tod_attack_miner import Miner
from tod_attack_miner.rpc.rpc import RPC
from tod_attack_miner.db.db import DB, EvaluationCandidate
from tod_attack_miner.db.filters import (
    get_filters_except_duplicate_limits,
    get_filters_duplicate_limits,
    get_filters_up_to_indirect_dependencies,
)

from t_race.commands.check import load_tod_candidates
from t_race.commands.defaults import DEFAULTS
from t_race.timing.time_tracker import TimeTracker


@dataclass
class BlockRange:
    start: int
    end: int


def block_range_type(input: str) -> BlockRange:
    try:
        start, end = input.split("-")
        start = int(start) if "0x" not in start else int(start, 16)
        end = int(end) if "0x" not in end else int(end, 16)
        if start > end:
            raise argparse.ArgumentTypeError(
                "Invalid block range: start may not be higher than end"
            )
        return BlockRange(start, end)
    except ValueError as e:
        error_message = (
            f'Invalid block range format: "{input}". '
            + "Expected block numbers in format: {start}-{inclusiveEnd}"
        )
        raise argparse.ArgumentTypeError(error_message) from e


def init_parser_mine(parser: ArgumentParser):
    parser.add_argument(
        "--version",
        action="version",
        version="tod_attack_miner " + version("tod_attack_miner"),
    )
    parser.add_argument(
        "--blocks",
        type=block_range_type,
        help="block range, eg 1000-1099. End is included",
        required=True,
    )
    parser.add_argument(
        "--window-size",
        type=int,
        default=None,
        help="If passed, filter TOD candidates that are {window-size} or more blocks apart",
    )
    parser.add_argument(
        "--duplicates-limit",
        type=int,
        default=None,
        help="If passed, limit the amount of collisions per address/code/family to this number",
    )
    parser.add_argument(
        "--quick-stats", action="store_true", help="Only compute performacnt stats"
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULTS.TOD_CANDIDATES_CSV_PATH,
        help="Path to the mined TOD",
    )
    parser.add_argument(
        "--output-stats-path",
        type=Path,
        default=DEFAULTS.TOD_MINER_STATS_PATH,
        help="Path where the stats will be stored",
    )
    parser.add_argument(
        "--evaluate-candidates-csv",
        type=Path,
        help="If passed, track what filtered these candidates and store it in --evaluation-csv",
    )
    parser.add_argument(
        "--evaluation-csv",
        type=Path,
        default=DEFAULTS.TOD_MINING_EVALUATION_CSV_PATH,
        help="See --evaluate-candidates-csv",
    )
    parser.add_argument(
        "--extract-indirect-dependencies",
        action="store_true",
        help="For the evaluation candidates, extract the indirect dependencies and stop further mining",
    )
    parser.add_argument("--postgres-user", type=str, default="postgres")
    parser.add_argument("--postgres-password", type=str, default="password")
    parser.add_argument("--postgres-host", type=str, default="localhost")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.set_defaults(func=mine_command)


def mine_command(args: Namespace, time_tracker: TimeTracker):
    output_path = args.base_dir / args.output_path
    output_stats_path = args.base_dir / args.output_stats_path
    evaluation_candidates_csv: Path | None = args.evaluate_candidates_csv
    evaluation_csv = args.base_dir / args.evaluation_csv
    extract_indirect_dependencies: bool = args.extract_indirect_dependencies

    assert (
        not evaluation_candidates_csv or evaluation_candidates_csv.exists()
    ), f"Could not find evaluation candidates: {evaluation_candidates_csv.absolute()}"

    conn_str = f"user={args.postgres_user} password={args.postgres_password} host={args.postgres_host} port={args.postgres_port}"
    print("Connecting to postgres: ", conn_str)

    with time_tracker.task(("mine",)):
        mine(
            args.blocks,
            args.window_size,
            args.duplicates_limit,
            output_path,
            output_stats_path,
            conn_str,
            args.provider,
            args.quick_stats,
            evaluation_candidates_csv,
            evaluation_csv,
            extract_indirect_dependencies,
            time_tracker,
        )


def mine(
    block_range: BlockRange,
    window_size: int | None,
    duplicates_limit: int | None,
    output_path: Path,
    output_stats_path: Path,
    conn_str: str,
    provider: str,
    quick_stats: bool,
    evaluate_candidates_csv_path: Path | None,
    evaluation_csv_path: Path,
    extract_indirect_dependencies: bool,
    time_tracker: TimeTracker,
):
    with psycopg.connect(conn_str) as conn:
        conn._check_connection_ok()
        miner = Miner(RPC(provider), DB(conn))

        miner.reset_db()

        with time_tracker.task(("mine", "fetch")):
            miner.fetch(block_range.start, block_range.end)

        if duplicates_limit is not None:
            with time_tracker.task(("mine", "skelcodes")):
                miner.compute_skelcodes()

        with time_tracker.task(("mine", "candidates")):
            print("Finding TOD candidates...", end="\r")
            miner.find_collisions()
            print(f"Found {miner.count_candidates()} TOD candidates")

        with time_tracker.task(("mine", "filter")):
            print("Filtering TOD candidates...", end="\r")
            print(
                f"Filter config: window_size={window_size}, duplicates_limit={duplicates_limit}"
            )
            filters = get_filters_except_duplicate_limits(window_size=window_size)
            if duplicates_limit is not None:
                filters += get_filters_duplicate_limits(limit=duplicates_limit)
            if evaluate_candidates_csv_path:
                evaluation_candidates = load_tod_candidates(
                    evaluate_candidates_csv_path
                )
                if extract_indirect_dependencies:
                    filters = get_filters_up_to_indirect_dependencies(window_size)
                    results = miner.get_indirect_dependencies(
                        filters, evaluation_candidates
                    )
                    print(f"Saving indirect dependencies to {evaluation_csv_path}")
                    save_indirect_dependencies(evaluation_csv_path, results)
                else:
                    results = miner.evaluate_candidates(filters, evaluation_candidates)
                    print(f"Saving evaluation results to {evaluation_csv_path}")
                    save_evaluation_results(evaluation_csv_path, results)
            else:
                miner.filter_candidates(filters)
            print(f"Reduced to {miner.count_candidates()} TOD candidates")

        with time_tracker.task(("mine", "save_candidates")):
            candidates = miner.get_candidates()
            for c in candidates:
                c["types"] = "|".join(c["types"])  # type: ignore

            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "tx_a",
                        "tx_b",
                        "block_dist",
                        "types",
                    ],
                )
                writer.writeheader()
                writer.writerows(candidates)

            print(f"Wrote {len(candidates)} TODs to {output_path}")

        with time_tracker.task(("mine", "stats")):
            print("Preparing stats...", end="\r")
            stats = miner.get_stats(quick_stats)

            with open(output_stats_path, "w") as f:
                json.dump(stats, f, indent=2)

            print(f"Wrote stats to {output_stats_path}")


def save_evaluation_results(
    results_csv_path: Path, results: Iterable[EvaluationCandidate]
):
    with open(results_csv_path, "w") as f:
        csv_writer = csv.DictWriter(f, ["tx_a", "tx_b", "filtered_by"])
        csv_writer.writeheader()
        rows = [
            {
                "tx_a": c["tx_a"],
                "tx_b": c["tx_b"],
                "filtered_by": c["filter"] or "",
            }
            for c in results
        ]
        csv_writer.writerows(rows)


def save_indirect_dependencies(
    results_csv_path: Path, results: Iterable[tuple[str, str, str]]
):
    with open(results_csv_path, "w") as f:
        csv_writer = csv.DictWriter(f, ["tx_a", "tx_b", "path"])
        csv_writer.writeheader()
        rows = [
            {
                "tx_a": tx_a,
                "tx_b": tx_b,
                "path": path,
            }
            for tx_a, tx_b, path in results
        ]
        csv_writer.writerows(rows)
