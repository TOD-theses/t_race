from argparse import ArgumentParser, Namespace
from importlib.metadata import version
from pathlib import Path


from t_race.commands.defaults import DEFAULTS
from t_race_stats.stats import process_stats


def init_parser_stats(parser: ArgumentParser):
    parser.add_argument(
        "--version",
        action="version",
        version="t_race_stats " + version("t_race_stats"),
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULTS.STATS_PATH,
        help="Path where statistics will be stored",
    )
    parser.set_defaults(func=stats_command, timing=False)


def stats_command(args: Namespace):
    output_dir = args.base_dir / args.output_path

    process_stats(args.base_dir, output_dir)
