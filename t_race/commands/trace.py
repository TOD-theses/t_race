from argparse import ArgumentParser, Namespace
import csv
from dataclasses import dataclass
from multiprocessing import Pool
from pathlib import Path
import subprocess
from typing import Iterable, Sequence

from tqdm import tqdm

from t_race.commands.defaults import DEFAULTS

REVM_REPLAYER_PATH = Path("revm-replayer")


def init_parser_trace(parser: ArgumentParser):
    parser.add_argument(
        "--version",
        action="version",
        version=f"revm-replayer <unknown - run '{REVM_REPLAYER_PATH} --version' instead>",
    )
    parser.add_argument(
        "--transactions-csv",
        type=Path,
        default=DEFAULTS.TOD_CANDIDATES_CSV_PATH,
        help="Path to a CSV file containing tx_a,tx_b pairs to trace",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULTS.TRACES_PATH,
        help="Directory where the traces should be stored",
    )
    parser.set_defaults(func=trace_command)


def trace_command(args: Namespace):
    transactions_csv_path: Path = args.base_dir / args.transactions_csv
    traces_dir: Path = args.base_dir / args.output_path

    traces_dir.mkdir(exist_ok=True)

    transactions = load_transactions(transactions_csv_path)

    process_inputs = [
        TraceArgs((tx_a, tx_b), traces_dir / f"{tx_a}_{tx_b}", args.provider)
        for tx_a, tx_b in transactions
    ]

    with Pool(args.max_workers) as p:
        for _ in tqdm(
            p.imap_unordered(create_trace, process_inputs, chunksize=1),
            total=len(process_inputs),
        ):
            pass


@dataclass
class TraceArgs:
    transaction_hashes: tuple[str, str]
    traces_dir: Path
    provider: str


def create_trace(args: TraceArgs):
    args.traces_dir.mkdir()
    run_replayer(
        args.provider, REVM_REPLAYER_PATH, args.transaction_hashes, args.traces_dir
    )


def load_transactions(csv_path: Path) -> Sequence[tuple[str, str]]:
    with open(csv_path, "r", newline="") as f:
        reader = csv.reader(f)
        return [(a, b) for a, b in reader]


def run_replayer(
    archive_node_provider: str,
    replayer_exe_path: Path,
    transactions: Iterable[str],
    output_dir: Path,
):
    args = [
        str(replayer_exe_path),
        "--archive-node-provider-url",
        archive_node_provider,
        "--output-dir",
        str(output_dir),
        "--transaction-hashes",
        *transactions,
    ]
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
