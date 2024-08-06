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

from tod_checker.checker.checker import (
    TodChecker,
    ReplayDivergedException,
    InsufficientEtherReplayException,
    TODCheckResult,
)
from tod_checker.rpc.rpc import RPC, OverridesFormatter
from tod_checker.executor.executor import TransactionExecutor
from tod_checker.state_changes.fetcher import StateChangesFetcher
from tod_checker.tx_block_mapper.tx_block_mapper import TransactionBlockMapper

checker: TodChecker | None = None
TODMethod = Literal["approximation"] | Literal["overall"]
TODResult = (
    Literal["TOD"]
    | Literal["not TOD"]
    | Literal["replay diverged"]
    | Literal["insufficient ether replay detected"]
    | Literal["insufficient ether replay error"]
    | Literal["error"]
)


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
        "--tod-method",
        choices=("approximation", "overall"),
        default=DEFAULTS.TOD_METHOD,
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
    parser.add_argument(
        "--results-details-jsonl",
        type=Path,
        default=DEFAULTS.TOD_CHECK_JSONL_PATH,
        help="File where the additional info for the results should be stored",
    )
    parser.set_defaults(func=check_command)


def check_command(args: Namespace, time_tracker: TimeTracker):
    transactions_csv_path: Path = args.base_dir / args.tod_candidates_csv
    tod_check_results_file_path: Path = args.base_dir / args.results_csv
    tod_check_details_file_path: Path = args.base_dir / args.results_details_jsonl
    tod_method = args.tod_method
    create_traces: bool = args.create_traces
    traces_directory_path: Path = args.base_dir / args.traces_dir
    traces_provider: str = args.traces_provider or args.provider

    checker = create_checker(args.provider)

    with time_tracker.task(("check",)):
        check(
            checker,
            transactions_csv_path,
            tod_check_results_file_path,
            tod_check_details_file_path,
            tod_method,
            args.max_workers,
            time_tracker,
        )

    if create_traces:
        change_checker_executor_provider(checker, traces_provider)

        with time_tracker.task(("trace",)):
            trace(
                checker,
                tod_check_results_file_path,
                traces_directory_path,
                args.max_workers,
                time_tracker,
            )


def check(
    checker_param: TodChecker,
    tod_candidates_path: Path,
    tod_check_results_path: Path,
    tod_check_details_path: Path,
    tod_method: TODMethod,
    max_workers: int,
    time_tracker: TimeTracker,
):
    global checker
    checker = checker_param
    transaction_pairs = load_tod_candidates(tod_candidates_path)

    blocks = set()
    with time_tracker.task(("check", "download transactions")):
        for tx in tqdm(set(flatten(transaction_pairs)), desc="Fetch transactions"):
            blocks.add(checker.download_data_for_transaction(tx))
    with time_tracker.task(("check", "fetch state changes")):
        for block in tqdm(blocks, desc="Fetch state changes"):
            checker.download_data_for_block(block)

    with time_tracker.task(("check", "check")):
        with open(tod_check_results_path, "w", newline="") as csv_file, open(
            tod_check_details_path, "w"
        ) as details_file:
            writer = csv.DictWriter(csv_file, ["tx_a", "tx_b", "result"])
            writer.writeheader()
            with ThreadPool(max_workers) as p:
                process_inputs = [
                    CheckArgs((tx_a, tx_b), tod_method)
                    for tx_a, tx_b in transaction_pairs
                ]
                for result in tqdm(
                    p.imap_unordered(check_candidate, process_inputs, chunksize=1),
                    total=len(process_inputs),
                    desc="Check TOD",
                ):
                    time_tracker.save_time_ms(
                        ("check", "check", result.id), result.elapsed_ms
                    )
                    tx_a, tx_b = result.id.split("_")
                    writer.writerow(
                        {
                            "tx_a": tx_a,
                            "tx_b": tx_b,
                            "result": result.result,
                        }
                    )
                    details: dict = {
                        "tx_a": tx_a,
                        "tx_b": tx_b,
                        "details": None,
                        "failure": None,
                    }
                    if result.details:
                        details["details"] = result.details.as_dict()
                    if result.result not in ("TOD", "not TOD"):
                        details["failure"] = result.result
                    details_file.write(json.dumps(details) + "\n")


def trace(
    checker_param: TodChecker,
    tod_check_results_path: Path,
    traces_directory_path: Path,
    max_workers: int,
    time_tracker: TimeTracker,
):
    global checker
    checker = checker_param

    traces_directory_path.mkdir(exist_ok=True)
    tods = load_tod_candidates(tod_check_results_path)

    with time_tracker.task(("trace",)):
        with ThreadPool(max_workers) as p:
            process_inputs = [
                TraceArgs((tx_a, tx_b), traces_directory_path) for tx_a, tx_b in tods
            ]
            for result in tqdm(
                p.imap_unordered(trace_tod, process_inputs, chunksize=1),
                total=len(process_inputs),
                desc="Trace scenarios",
            ):
                time_tracker.save_time_ms(("trace", result.id), result.elapsed_ms)
                if result.error:
                    print(result.error)


def create_checker(provider: str):
    rpc = RPC(provider, OverridesFormatter("old Erigon"))
    state_changes_fetcher = StateChangesFetcher(rpc)
    tx_block_mapper = TransactionBlockMapper(rpc)
    simulator = TransactionExecutor(rpc)
    return TodChecker(simulator, state_changes_fetcher, tx_block_mapper)


def change_checker_executor_provider(checker: TodChecker, provider: str):
    checker.executor._rpc = RPC(provider, OverridesFormatter("old Erigon"))


@dataclass
class CheckArgs:
    transaction_hashes: tuple[str, str]
    tod_method: TODMethod


@dataclass
class CheckResult:
    id: str
    result: TODResult
    details: TODCheckResult | None
    elapsed_ms: int


def check_candidate(args: CheckArgs):
    res = None

    with StopWatch() as stopwatch:
        global checker
        assert checker is not None
        tx_a, tx_b = args.transaction_hashes
        try:
            res = checker.check(
                tx_a,
                tx_b,
            )
            if args.tod_method == "approximation":
                result = "TOD" if res.tx_b_comparison.differences() else "not TOD"
            else:
                result = "TOD" if res.overall_comparison.differences() else "not TOD"
        except ReplayDivergedException:
            result = "replay diverged"
        except InsufficientEtherReplayException:
            result = "insufficient ether replay detected"
        except Exception as e:
            if "insufficient funds" in str(e).lower():
                result = "insufficient ether replay error"
            else:
                result = "error"

    return CheckResult(
        f"{args.transaction_hashes[0]}_{args.transaction_hashes[1]}",
        result,  # type: ignore
        res,
        stopwatch.elapsed_ms(),
    )


@dataclass
class TraceArgs:
    transaction_hashes: tuple[str, str]
    traces_dir: Path


@dataclass
class TraceResult:
    id: str
    error: Exception | None
    elapsed_ms: int


def trace_tod(args: TraceArgs) -> TraceResult:
    error = None
    tx_a, tx_b = args.transaction_hashes
    id = f"{tx_a}_{tx_b}"

    with StopWatch() as stopwatch:
        global checker
        assert checker is not None
        try:
            output_dir = args.traces_dir / id
            output_dir.mkdir(exist_ok=True)
            traces_normal_dir = output_dir / "actual"
            traces_reverse_dir = output_dir / "reverse"
            traces_normal_dir.mkdir(exist_ok=True)
            traces_reverse_dir.mkdir(exist_ok=True)

            with open(output_dir / "metadata.json", "w") as metadata_file:
                tx_a_data = checker._tx_block_mapper.get_transaction(tx_a)
                tx_b_data = checker._tx_block_mapper.get_transaction(tx_b)
                tx_a_data["value"] = hex(tx_a_data["value"])  # type: ignore
                tx_b_data["value"] = hex(tx_b_data["value"])  # type: ignore
                metadata = {
                    "id": id,
                    "transactions": {
                        tx_a: checker._tx_block_mapper.get_transaction(tx_a),
                        tx_b: checker._tx_block_mapper.get_transaction(tx_b),
                    },
                    "transactions_order": [tx_a, tx_b],
                }
                json.dump(metadata, metadata_file)

            traces = checker.trace_both_scenarios(tx_a, tx_b)
            trace_normal_b, trace_reverse_b, trace_normal_a, trace_reverse_a = traces

            with open(traces_normal_dir / f"{tx_a}.json", "w") as f:
                json.dump(trace_normal_a, f)
            with open(traces_reverse_dir / f"{tx_a}.json", "w") as f:
                json.dump(trace_reverse_a, f)
            with open(traces_normal_dir / f"{tx_b}.json", "w") as f:
                json.dump(trace_normal_b, f)
            with open(traces_reverse_dir / f"{tx_b}.json", "w") as f:
                json.dump(trace_reverse_b, f)

        except Exception as e:
            error = e

    return TraceResult(
        id,
        error,
        stopwatch.elapsed_ms(),
    )


def load_tod_candidates(csv_path: Path) -> Sequence[tuple[str, str]]:
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return [(row["tx_a"], row["tx_b"]) for row in reader]


def load_tod_transactions(csv_path: Path) -> Sequence[tuple[str, str]]:
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return [(row["tx_a"], row["tx_b"]) for row in reader if row["result"] == "TOD"]


def flatten(nested_list: Iterable[Iterable]) -> list:
    return [element for sublist in nested_list for element in sublist]
