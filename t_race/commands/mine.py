from argparse import ArgumentParser, Namespace
import argparse
import csv
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

from tod_attack_miner import Miner


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
        "--db-path",
        type=Path,
        default=Path("miner.db"),
        help="Path to the database file containing prestate traces and more",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("mined_tods.csv"),
        help="Path to the mined TOD",
    )
    parser.set_defaults(func=mine)


def mine(args: Namespace):
    db_path = args.base_dir / args.db_path
    output_path = args.base_dir / args.output_path

    miner = Miner(args.provider, db_path)

    block_range: BlockRange = args.blocks
    print(f"Fetching block data for {block_range}")
    miner.fetch(block_range.start, block_range.end)

    print("Getting TODs from block data")
    attacks = miner.get_attacks(block_range.start, block_range.end)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(attacks)

    print(f"Wrote {len(attacks)} TODs to {output_path}")
