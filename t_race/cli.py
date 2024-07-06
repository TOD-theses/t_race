"""CLI interface for t_race project."""

from argparse import ArgumentParser
from importlib.metadata import version
from os import cpu_count
from pathlib import Path

from t_race.commands.analyze import init_parser_analyze
from t_race.commands.mine import init_parser_mine
from t_race.commands.run import init_parser_run
from t_race.commands.trace import init_parser_trace


def main():
    parser = ArgumentParser(description="Find and analyze TOD transactions in Ethereum")
    parser.add_argument(
        "--version", action="version", version="%(prog)s " + version("t_race")
    )
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

    subparsers = parser.add_subparsers(required=True, title="Commands")

    parser_run = subparsers.add_parser("run", help="Run all T-Race steps")
    init_parser_run(parser_run)

    parser_mine = subparsers.add_parser(
        "mine", help="Mine TOD candidates from Ethereum history"
    )
    init_parser_mine(parser_mine)

    parser_trace = subparsers.add_parser(
        "trace", help="Generate traces for TOD candidates"
    )
    init_parser_trace(parser_trace)

    parser_analyzer = subparsers.add_parser("analyze", help="Analyze traces")
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
