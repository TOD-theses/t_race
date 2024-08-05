from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from importlib.metadata import version
import json
from multiprocessing import Pool
from pathlib import Path
import traceback

from tqdm import tqdm


from t_race.commands.analyze import (
    analyze_attack,
    save_evaluations,
)
from t_race.commands.check import (
    change_checker_executor_provider,
    check,
    create_checker,
    load_tod_transactions,
)
from t_race.commands.defaults import DEFAULTS
from t_race.commands.mine import block_range_type, mine
from t_race.timing.stopwatch import StopWatch
from t_race.timing.time_tracker import TimeTracker
from tod_checker.checker.checker import (
    TodChecker,
)
from traces_analyzer.loader.in_memory_loader import InMemoryLoader
from traces_analyzer.loader.event_parser import (
    VmTraceDictEventsParser,
)
from t_race_stats.stats import process_stats


def init_parser_run(parser: ArgumentParser):
    parser.add_argument(
        "--version", action="version", version="%(prog)s " + version("t_race")
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
        "--traces-provider",
        type=str,
        default=None,
        help="If specified, use this RPC provider to generate traces (heavier load) instead of the normal one",
    )
    parser.add_argument(
        "--duplicates-limit",
        type=int,
        default=None,
        help="If passed, limit the amount of collisions per address/code/family to this number",
    )
    parser.add_argument("--postgres-user", type=str, default="postgres")
    parser.add_argument("--postgres-password", type=str, default="password")
    parser.add_argument("--postgres-host", type=str, default="localhost")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.set_defaults(func=run_command, timing=False)


def run_command(args: Namespace):
    traces_provider: str = args.traces_provider or args.provider

    with TimeTracker(args.base_dir / args.timings_output, "t_race") as time_tracker:
        with time_tracker.task(("t_race",)):
            with time_tracker.task(("mine",)):
                run_mining(args, time_tracker)

            checker = create_checker(args.provider)
            with time_tracker.task(("check",)):
                run_check(args, time_tracker, checker)

            with time_tracker.task(("trace_analyze",)):
                run_trace_analyze(
                    args.base_dir,
                    args.max_workers,
                    args.provider,
                    traces_provider,
                    time_tracker,
                )

    process_stats(args.base_dir, args.base_dir / DEFAULTS.STATS_PATH)


def run_mining(args: Namespace, time_tracker: TimeTracker):
    output_path = args.base_dir / DEFAULTS.TOD_CANDIDATES_CSV_PATH
    output_stats_path = args.base_dir / DEFAULTS.TOD_MINER_STATS_PATH

    conn_str = f"user={args.postgres_user} password={args.postgres_password} host={args.postgres_host} port={args.postgres_port}"
    print("Connecting to postgres: ", conn_str)
    mine(
        args.blocks,
        args.window_size,
        args.duplicates_limit,
        output_path,
        output_stats_path,
        conn_str,
        args.provider,
        time_tracker,
    )


def run_check(args: Namespace, time_tracker: TimeTracker, checker: TodChecker):
    check(
        checker,
        args.base_dir / DEFAULTS.TOD_CANDIDATES_CSV_PATH,
        args.base_dir / DEFAULTS.TOD_CHECK_CSV_PATH,
        args.base_dir / DEFAULTS.TOD_CHECK_JSONL_PATH,
        DEFAULTS.TOD_METHOD,
        args.max_workers,
        time_tracker,
    )


def run_trace_analyze(
    base_dir: Path,
    max_workers: int,
    provider: str,
    traces_provider: str,
    time_tracker: TimeTracker,
):
    tod_check_csv_path: Path = base_dir / DEFAULTS.TOD_CHECK_CSV_PATH
    results_dir: Path = base_dir / DEFAULTS.RESULTS_PATH
    results_dir.mkdir(exist_ok=True)
    traces_dir: Path = base_dir / DEFAULTS.TRACES_PATH
    traces_dir.mkdir(exist_ok=True)

    transactions = load_tod_transactions(tod_check_csv_path)

    process_inputs = [
        TraceAnalyzeArgs(tx_pair, results_dir, provider, traces_provider)
        for tx_pair in transactions
    ]

    with Pool(max_workers) as p:
        for result in tqdm(
            p.imap_unordered(trace_analyze, process_inputs, chunksize=1),
            desc="Trace and analyze TOD candidates",
            total=len(process_inputs),
        ):
            time_tracker.save_time_ms(("trace", result.id), result.ms_trace)
            time_tracker.save_time_ms(("analyze", result.id), result.ms_analyze)


@dataclass
class TraceAnalyzeArgs:
    transactions: tuple[str, str]
    results_directory: Path
    provider: str
    traces_provider: str


@dataclass
class TraceAnalyzeResult:
    id: str
    error_trace: bool
    error_analyze: bool
    ms_trace: int
    ms_analyze: int


def trace_analyze(args: TraceAnalyzeArgs) -> TraceAnalyzeResult:
    tx_a, tx_b = args.transactions
    id = f"{tx_a}_{tx_b}"
    out_path = args.results_directory / f"{id}.json"
    error_trace = False
    error_analyze = False
    trace_normal_b = None
    trace_reverse_b = None
    checker = create_checker(args.provider)
    with StopWatch() as stopwatch_traces:
        try:
            blocks = set()
            blocks.add(checker.download_data_for_transaction(tx_a))
            blocks.add(checker.download_data_for_transaction(tx_b))
            for b in blocks:
                checker.download_data_for_block(b)

            tx_b_data = checker._tx_block_mapper.get_transaction(tx_b)
            tx_b_data["value"] = hex(tx_b_data["value"])  # type: ignore

            change_checker_executor_provider(checker, args.traces_provider)

            trace_normal_b, trace_reverse_b, trace_normal_a, trace_reverse_a = (
                checker.trace_both_scenarios(tx_a, tx_b)
            )
        except Exception:
            error_trace = True
            msg = traceback.format_exc()
            with open(out_path, "w") as f:
                json.dump({"exception_trace": msg}, f)

    if error_trace:
        return TraceAnalyzeResult(
            id, error_trace, error_analyze, stopwatch_traces.elapsed_ms(), 0
        )

    with StopWatch() as stopwatch_analyze:
        try:
            with InMemoryLoader(
                id,
                tx_b_data,  # type: ignore
                trace_normal_b,
                trace_reverse_b,
                VmTraceDictEventsParser(),
            ) as bundle:
                evaluations = analyze_attack(bundle)
                save_evaluations(evaluations, out_path)
        except Exception:
            error_analyze = True
            msg = traceback.format_exc()
            with open(out_path, "w") as f:
                json.dump({"exception_analyze": msg}, f)

    return TraceAnalyzeResult(
        id,
        error_trace,
        error_analyze,
        stopwatch_traces.elapsed_ms(),
        stopwatch_analyze.elapsed_ms(),
    )
