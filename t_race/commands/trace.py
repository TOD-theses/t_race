from argparse import ArgumentParser, Namespace
import csv
from pathlib import Path
import subprocess
from typing import Iterable, Sequence

from tqdm import tqdm


def init_parser_trace(parser: ArgumentParser):
    parser.add_argument(
        "--transactions-csv",
        type=Path,
        default=Path("mined_tods.csv"),
        help="Path to a CSV file containing tx_a,tx_b pairs to trace",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("traces"),
        help="Directory where the traces should be stored",
    )
    parser.set_defaults(func=trace)


def trace(args: Namespace):
    transactions_csv_path: Path = args.base_dir / args.transactions_csv
    traces_dir: Path = args.base_dir / args.output_path

    traces_dir.mkdir(exist_ok=True)

    transactions = load_transactions(transactions_csv_path)

    for tx_a, tx_b in tqdm(transactions):
        tod_dir = traces_dir / f"{tx_a}_{tx_b}"
        tod_dir.mkdir()
        run_replayer(args.provider, Path("revm-replayer"), (tx_a, tx_b), tod_dir)


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
