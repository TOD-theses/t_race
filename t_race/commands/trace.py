from argparse import ArgumentParser, Namespace
import csv
from multiprocessing import Pool
from pathlib import Path
import subprocess
from typing import Iterable, Sequence

from tqdm import tqdm

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

    process_inputs = [
        ((tx_a, tx_b), traces_dir / f"{tx_a}_{tx_b}", args.provider)
        for tx_a, tx_b in transactions
    ]

    with Pool(args.max_workers) as p:
        for _ in tqdm(
            p.imap_unordered(create_trace, process_inputs, chunksize=1),
            total=len(process_inputs),
        ):
            pass


def create_trace(args: tuple[tuple[str, str], Path, str]):
    transactions, tod_dir, provider = args

    tod_dir.mkdir()
    run_replayer(provider, REVM_REPLAYER_PATH, transactions, tod_dir)
    return f"Created {transactions}"


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
