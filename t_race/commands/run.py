from argparse import ArgumentParser, Namespace
from importlib.metadata import version
from multiprocessing import Pool
from pathlib import Path
import shutil

from tqdm import tqdm


from t_race.commands.analyze import AnalyzeArgs, analyze
from t_race.commands.defaults import DEFAULTS
from t_race.commands.mine import block_range_type, mine
from t_race.commands.trace import TraceArgs, create_trace, load_transactions


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
    parser.add_argument("--postgres-user", type=str, default="postgres")
    parser.add_argument("--postgres-password", type=str, default="password")
    parser.add_argument("--postgres-host", type=str, default="localhost")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.set_defaults(func=run_command)


def run_command(args: Namespace):
    run_mining(args)
    run_trace_analyze(args)


def run_mining(args: Namespace):
    output_path = args.base_dir / DEFAULTS.TOD_CANDIDATES_CSV_PATH
    output_stats_path = args.base_dir / DEFAULTS.TOD_MINER_STATS_PATH

    conn_str = f"user={args.postgres_user} password={args.postgres_password} host={args.postgres_host} port={args.postgres_port}"
    print("Connecting to postgres: ", conn_str)
    mine(
        args.blocks,
        args.window_size,
        output_path,
        output_stats_path,
        conn_str,
        args.provider,
    )


def run_trace_analyze(args: Namespace):
    tod_candidates_csv_path: Path = args.base_dir / DEFAULTS.TOD_CANDIDATES_CSV_PATH
    results_dir: Path = args.base_dir / DEFAULTS.RESULTS_PATH
    results_dir.mkdir(exist_ok=True)
    traces_dir: Path = args.base_dir / DEFAULTS.TRACES_PATH
    traces_dir.mkdir(exist_ok=True)

    transactions = load_transactions(tod_candidates_csv_path)

    process_inputs = [
        create_process_input(tx_a, tx_b, traces_dir, results_dir, args.provider)
        for tx_a, tx_b in transactions
    ]

    with Pool(args.max_workers) as p:
        for _ in tqdm(
            p.imap_unordered(trace_analyze, process_inputs, chunksize=1),
            desc="Trace and analyze TOD candidates",
            total=len(process_inputs),
        ):
            pass

    traces_dir.rmdir()


def create_process_input(
    tx_a: str, tx_b: str, traces_dir: Path, results_dir: Path, provider: str
) -> tuple[TraceArgs, AnalyzeArgs]:
    transaction_traces_dir = traces_dir / f"{tx_a}_{tx_b}"

    return (
        TraceArgs((tx_a, tx_b), transaction_traces_dir, provider),
        AnalyzeArgs(transaction_traces_dir, results_dir),
    )


def trace_analyze(args: tuple[TraceArgs, AnalyzeArgs]):
    trace_args, analyze_args = args

    create_trace(trace_args)
    analyze(analyze_args)

    shutil.rmtree(analyze_args.traces_path)
