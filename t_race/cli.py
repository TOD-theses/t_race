"""CLI interface for t_race project."""

from argparse import ArgumentParser
from importlib.metadata import version
from os import cpu_count
from pathlib import Path

from t_race.commands.analyze import init_parser_analyze
from t_race.commands.check import init_parser_check
from t_race.commands.defaults import DEFAULTS
from t_race.commands.mine import init_parser_mine
from t_race.commands.run import init_parser_run
from t_race.commands.stats import init_parser_stats
from t_race.commands.trace import init_parser_trace
from t_race.timing.time_tracker import TimeTracker


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
    parser.add_argument(
        "--timings-output",
        type=Path,
        default=DEFAULTS.TIMINGS_PATH,
        help="Path where timing statistics will be stored",
    )
    parser.set_defaults(timing=True)

    subparsers = parser.add_subparsers(required=True, title="Commands")

    parser_run = subparsers.add_parser("run", help="Run all T-Race steps")
    init_parser_run(parser_run)

    parser_mine = subparsers.add_parser(
        "mine", help="Mine TOD candidates from Ethereum history"
    )
    init_parser_mine(parser_mine)

    parser_check = subparsers.add_parser(
        "check", help="Check if TOD candidates are really TOD"
    )
    init_parser_check(parser_check)

    parser_trace = subparsers.add_parser(
        "trace", help="Generate traces for TOD candidates"
    )
    init_parser_trace(parser_trace)

    parser_analyzer = subparsers.add_parser("analyze", help="Analyze traces")
    init_parser_analyze(parser_analyzer)

    parser_stats = subparsers.add_parser("stats", help="Create stats")
    init_parser_stats(parser_stats)

    args = parser.parse_args()

    args.base_dir.mkdir(exist_ok=True)

    if args.timing:
        with TimeTracker(args.base_dir / args.timings_output) as time_tracker:
            with time_tracker.component("t_race"):
                args.func(args, time_tracker)
    else:
        args.func(args)


def default_workers() -> int:
    if not (cpus := cpu_count()):
        return 12
    return min(32, cpus + 4)
