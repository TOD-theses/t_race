from argparse import ArgumentParser, Namespace
import csv
from dataclasses import dataclass
from importlib.metadata import version
import json
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Iterable, Literal, Sequence

from tqdm import tqdm

from t_race.commands.defaults import DEFAULTS
from t_race.timing.stopwatch import StopWatch
from t_race.timing.time_tracker import TimeTracker

from tod_checker.checker.checker import TodChecker, ReplayDivergedException
from tod_checker.rpc.rpc import RPC, OverridesFormatter
from tod_checker.executor.executor import TransactionExecutor
from tod_checker.state_changes.fetcher import StateChangesFetcher
from tod_checker.tx_block_mapper.tx_block_mapper import TransactionBlockMapper

checker: TodChecker | None = None


def init_parser_check(parser: ArgumentParser):
    parser.add_argument(
        "--version",
        action="version",
        version=f"tod_checker {version('tod_checker')}",
    )
    parser.add_argument(
        "--tod-candidates-csv",
        type=Path,
        default=DEFAULTS.TOD_CANDIDATES_CSV_PATH,
        help="Path to a CSV file containing tx_a,tx_b pairs to trace",
    )
    parser.add_argument(
        "--create-traces",
        action="store_true",
        help="Create traces for every found TOD",
    )
    parser.add_argument(
        "--traces-provider",
        type=str,
        default=None,
        help="If specified, use this RPC provider to generate traces (heavier load) instead of the normal one",
    )
    parser.add_argument(
        "--traces-dir",
        type=Path,
        default=DEFAULTS.TRACES_PATH,
        help="Directory where the traces should be stored",
    )
    parser.add_argument(
        "--results-csv",
        type=Path,
        default=DEFAULTS.TOD_CHECK_CSV_PATH,
        help="File where the results should be stored",
    )
    parser.set_defaults(func=check_command)


def check_command(args: Namespace, time_tracker: TimeTracker):
    transactions_csv_path: Path = args.base_dir / args.tod_candidates_csv
    tod_check_results_file_path: Path = args.base_dir / args.results_csv
    create_traces: bool = args.create_traces
    traces_directory_path: Path = args.base_dir / args.traces_dir
    traces_provider: str = args.traces_provider or args.provider

    transaction_pairs = load_transactions(transactions_csv_path)

    rpc = RPC(args.provider, OverridesFormatter("old Erigon"))
    state_changes_fetcher = StateChangesFetcher(rpc)
    tx_block_mapper = TransactionBlockMapper(rpc)
    simulator = TransactionExecutor(rpc)
    # make it global, so it can be accessed by all threads
    # threads should make read-only accesses
    global checker
    checker = TodChecker(simulator, state_changes_fetcher, tx_block_mapper)

    print("Checking for TOD")

    tods: list[tuple[str, str]] = []

    with time_tracker.component("check"):
        blocks = set()
        with time_tracker.step("check", "download transactions"):
            for tx in tqdm(set(flatten(transaction_pairs)), desc="Fetch transactions"):
                blocks.add(checker.download_data_for_transaction(tx))
        with time_tracker.step("check", "fetch state changes"):
            for block in tqdm(blocks, desc="Fetching state changes"):
                checker.download_data_for_block(block)

        with open(tod_check_results_file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, ["tx_a", "tx_b", "result"])
            writer.writeheader()
            with ThreadPool(args.max_workers) as p:
                process_inputs = [
                    CheckArgs((tx_a, tx_b), args.provider)
                    for tx_a, tx_b in transaction_pairs
                ]
                for result in tqdm(
                    p.imap_unordered(check, process_inputs, chunksize=1),
                    total=len(process_inputs),
                    desc="Check TOD",
                ):
                    time_tracker.save_time_step_ms(
                        "check", result.id, result.elapsed_ms
                    )
                    tx_a, tx_b = result.id.split("_")
                    writer.writerow(
                        {
                            "tx_a": tx_a,
                            "tx_b": tx_b,
                            "result": result.result,
                        }
                    )
                    if result.result == "TOD":
                        tods.append((tx_a, tx_b))

    if create_traces:
        print("Creating execution traces")
        traces_rpc = RPC(traces_provider, OverridesFormatter("old Erigon"))
        traces_simulator = TransactionExecutor(traces_rpc)
        checker = TodChecker(traces_simulator, state_changes_fetcher, tx_block_mapper)

        traces_directory_path.mkdir(exist_ok=True)

        with time_tracker.component("trace"):
            with ThreadPool(args.max_workers) as p:
                process_inputs = [
                    TraceArgs((tx_a, tx_b), traces_directory_path, args.provider)
                    for tx_a, tx_b in tods
                ]
                for result in tqdm(
                    p.imap_unordered(trace, process_inputs, chunksize=1),
                    total=len(process_inputs),
                ):
                    time_tracker.save_time_step_ms(
                        "trace", result.id, result.elapsed_ms
                    )
                    if result.error:
                        print(result.error)


@dataclass
class CheckArgs:
    transaction_hashes: tuple[str, str]
    provider: str


@dataclass
class CheckResult:
    id: str
    result: (
        Literal["TOD"]
        | Literal["not TOD"]
        | Literal["replay diverged"]
        | Literal["error"]
    )
    elapsed_ms: int


def check(args: CheckArgs):
    with StopWatch() as stopwatch:
        global checker
        assert checker is not None
        tx_a, tx_b = args.transaction_hashes
        try:
            res = checker.is_TOD(tx_a, tx_b)
            if not res:
                result = "not TOD"
            else:
                result = "TOD"
        except ReplayDivergedException:
            result = "replay diverged"
        except Exception as e:
            print(e)
            result = "error"

    return CheckResult(
        f"{args.transaction_hashes[0]}_{args.transaction_hashes[1]}",
        result,  # type: ignore
        stopwatch.elapsed_ms(),
    )


@dataclass
class TraceArgs:
    transaction_hashes: tuple[str, str]
    output_dir: Path
    provider: str


@dataclass
class TraceResult:
    id: str
    error: Exception | None
    elapsed_ms: int


def trace(args: TraceArgs) -> TraceResult:
    error = None
    with StopWatch() as stopwatch:
        global checker
        assert checker is not None
        tx_a, tx_b = args.transaction_hashes
        try:
            trace_normal, trace_reverse = checker.trace_both_scenarios(tx_a, tx_b)
            with open(args.output_dir / f"{tx_a}_{tx_b}.json", "w") as f:
                json.dump(trace_normal, f)
            with open(args.output_dir / f"{tx_b}_{tx_a}.json", "w") as f:
                json.dump(trace_reverse, f)
        except Exception as e:
            error = e

    return TraceResult(
        f"{tx_a}_{tx_b}",  # type: ignore
        error,
        stopwatch.elapsed_ms(),
    )


def load_transactions(csv_path: Path) -> Sequence[tuple[str, str]]:
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return [(row["tx_a"], row["tx_b"]) for row in reader]


def flatten(nested_list: Iterable[Iterable]) -> list:
    return [element for sublist in nested_list for element in sublist]
