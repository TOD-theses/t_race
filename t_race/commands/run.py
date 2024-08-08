from argparse import ArgumentParser, Namespace
from importlib.metadata import version
from pathlib import Path


from t_race.commands.check import (
    check,
    check_properties,
    create_checker,
)
from t_race.commands.defaults import DEFAULTS
from t_race.commands.mine import block_range_type, mine
from t_race.timing.time_tracker import TimeTracker
from tod_checker.checker.checker import (
    TodChecker,
)
from t_race_stats.stats import process_stats


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
    parser.add_argument(
        "--duplicates-limit",
        type=int,
        default=None,
        help="If passed, limit the amount of collisions per address/code/family to this number",
    )
    parser.add_argument(
        "--extensive-stats", action="store_true", help="Include time consuming stats"
    )
    parser.add_argument("--postgres-user", type=str, default="postgres")
    parser.add_argument("--postgres-password", type=str, default="password")
    parser.add_argument("--postgres-host", type=str, default="localhost")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.set_defaults(func=run_command, timing=False)


def run_command(args: Namespace):
    with TimeTracker(args.base_dir / args.timings_output, "t_race") as time_tracker:
        with time_tracker.task(("t_race",)):
            with time_tracker.task(("mine",)):
                run_mining(args, time_tracker)

            checker = create_checker(args.provider)
            with time_tracker.task(("check",)):
                run_check(args, time_tracker, checker)

            with time_tracker.task(("properties",)):
                run_properties(args, time_tracker, checker)

    process_stats(args.base_dir, args.base_dir / DEFAULTS.STATS_PATH)


def run_mining(args: Namespace, time_tracker: TimeTracker):
    output_path = args.base_dir / DEFAULTS.TOD_CANDIDATES_CSV_PATH
    output_stats_path = args.base_dir / DEFAULTS.TOD_MINER_STATS_PATH

    conn_str = f"user={args.postgres_user} password={args.postgres_password} host={args.postgres_host} port={args.postgres_port}"
    print("Connecting to postgres: ", conn_str)
    mine(
        args.blocks,
        args.window_size,
        args.duplicates_limit,
        output_path,
        output_stats_path,
        conn_str,
        args.provider,
        not args.extensive_stats,
        None,
        Path(),
        time_tracker,
    )


def run_check(args: Namespace, time_tracker: TimeTracker, checker: TodChecker):
    check(
        checker,
        args.base_dir / DEFAULTS.TOD_CANDIDATES_CSV_PATH,
        args.base_dir / DEFAULTS.TOD_CHECK_CSV_PATH,
        args.base_dir / DEFAULTS.TOD_CHECK_JSONL_PATH,
        DEFAULTS.TOD_METHOD,
        args.max_workers,
        time_tracker,
    )


def run_properties(args: Namespace, time_tracker: TimeTracker, checker: TodChecker):
    check_properties(
        checker,
        args.base_dir / DEFAULTS.TOD_CHECK_CSV_PATH,
        args.base_dir / DEFAULTS.TOD_PROPERTIES_CSV_PATH,
        args.base_dir / DEFAULTS.TOD_PROPERTIES_JSONL_PATH,
        args.max_workers,
        time_tracker,
    )
