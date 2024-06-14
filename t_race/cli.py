"""CLI interface for t_race project."""

from argparse import ArgumentParser
from os import cpu_count
from pathlib import Path

from t_race.commands.analyze import init_parser_analyze
from t_race.commands.mine import init_parser_mine
from t_race.commands.trace import init_parser_trace


def main():
    parser = ArgumentParser(description="Find and analyze TOD transactions in Ethereum")
    parser.add_argument(
        "--provider",
        type=str,
        default="http://localhost:8124/eth",
        help="Url to the archive RPC provider",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("out"),
        help="Directory where artifacts will be stored. Prepended to all other paths. Defaults to ./out",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=default_workers(),
        help="Maximum number of parallel processes to use",
    )

    subparsers = parser.add_subparsers(required=True, title="Command")

    parser_mine = subparsers.add_parser("mine", help="mine help")
    init_parser_mine(parser_mine)

    parser_trace = subparsers.add_parser("trace", help="trace help")
    init_parser_trace(parser_trace)

    parser_analyzer = subparsers.add_parser("analyze", help="analyze help")
    init_parser_analyze(parser_analyzer)

    args = parser.parse_args()

    out: Path = args.base_dir
    out.mkdir(exist_ok=True)

    # call subcommand
    args.func(args)


def default_workers() -> int:
    if not (cpus := cpu_count()):
        return 12
    return min(32, cpus + 4)
