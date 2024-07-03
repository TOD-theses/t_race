from argparse import ArgumentParser, Namespace
import argparse
import csv
from dataclasses import dataclass
from importlib.metadata import version
import json
from pathlib import Path

import psycopg
from tod_attack_miner import Miner
from tod_attack_miner.rpc.rpc import RPC
from tod_attack_miner.db.db import DB


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
        "--output-path",
        type=Path,
        default=Path("mined_tods.csv"),
        help="Path to the mined TOD",
    )
    parser.add_argument(
        "--output-stats-path",
        type=Path,
        default=Path("mining_stats.json"),
        help="Path where the stats will be stored",
    )
    parser.add_argument("--postgres-user", type=str, default="postgres")
    parser.add_argument("--postgres-password", type=str, default="password")
    parser.add_argument("--postgres-host", type=str, default="localhost")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.set_defaults(func=mine)


def mine(args: Namespace):
    output_path = args.base_dir / args.output_path
    output_stats_path = args.base_dir / args.output_stats_path

    conn_str = f"user={args.postgres_user} password={args.postgres_password} host={args.postgres_host} port={args.postgres_port}"
    print("Connecting to postgres: ", conn_str)

    with psycopg.connect(conn_str) as conn:
        conn._check_connection_ok()
        miner = Miner(RPC(args.provider), DB(conn))

        block_range: BlockRange = args.blocks
        miner.fetch(block_range.start, block_range.end)
        print("Finding TOD candidates...", end="\r")
        miner.find_conflicts()
        print(f"Found {miner.count_candidates()} TOD candidates")
        print("Filtering TOD candidates...", end="\r")
        miner.filter_candidates()
        print(f"Reduced to {miner.count_candidates()} TOD candidates")

        candidates = miner.get_candidates()

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(candidates)

        print(f"Wrote {len(candidates)} TODs to {output_path}")

        print("Getting stats for the mining process...")
        stats = miner.get_stats()

        with open(output_stats_path, "w") as f:
            json.dump(stats, f, indent=2)

        print(f"Wrote stats to {output_stats_path}")
