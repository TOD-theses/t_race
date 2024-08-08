from argparse import ArgumentParser, Namespace
import csv
from dataclasses import dataclass
from importlib.metadata import version
import json
from multiprocessing.pool import ThreadPool
from pathlib import Path
import traceback
from typing import Iterable, Literal, Sequence

from tqdm import tqdm

from t_race.commands.defaults import DEFAULTS
from t_race.timing.stopwatch import StopWatch
from t_race.timing.time_tracker import TimeTracker

from tod_checker.checker.checker import (
    TodChecker,
    ReplayDivergedException,
    InsufficientEtherReplayException,
    TODCheckResult,
)
from tod_checker.rpc.rpc import RPC, OverridesFormatter
from tod_checker.executor.executor import TransactionExecutor
from tod_checker.state_changes.fetcher import StateChangesFetcher
from tod_checker.tx_block_mapper.tx_block_mapper import TransactionBlockMapper
from tod_checker.currency_changes.properties.gain_and_loss import (
    check_gain_and_loss_properties,
    GainAndLossResult,
)
from tod_checker.currency_changes.properties.securify import (
    check_securify_properties,
    SecurifyCheckResult,
)
from tod_checker.currency_changes.properties.erc20_approve_after_transfer import (
    check_erc20_approval_attack,
    ERC20ApprovalCheckResult,
)
from tod_checker.currency_changes.tracer.currency_changes_js_tracer import (
    CurrencyChangesJSTracer,
)

checker: TodChecker | None = None
TODMethod = Literal["approximation"] | Literal["overall"]
TODResult = (
    Literal["TOD"]
    | Literal["not TOD"]
    | Literal["replay diverged"]
    | Literal["insufficient ether replay detected"]
    | Literal["insufficient ether replay error"]
    | Literal["error"]
)


def init_parser_check(parser: ArgumentParser):
    parser.add_argument(
        "--version",
        action="version",
        version=f"tod_checker {version('tod_checker')}",
    )
    parser.add_argument(
        "--tod-candidates-csv",
        type=Path,
        default=DEFAULTS.TOD_CANDIDATES_CSV_PATH,
        help="Path to a CSV file containing tx_a,tx_b pairs to check",
    )
    parser.add_argument(
        "--tod-method",
        choices=("approximation", "overall"),
        default=DEFAULTS.TOD_METHOD,
    )
    parser.add_argument(
        "--results-csv",
        type=Path,
        default=DEFAULTS.TOD_CHECK_CSV_PATH,
        help="File where the results should be stored",
    )
    parser.add_argument(
        "--results-details-jsonl",
        type=Path,
        default=DEFAULTS.TOD_CHECK_JSONL_PATH,
        help="File where the additional info for the results should be stored",
    )
    parser.add_argument(
        "--properties-csv",
        type=Path,
        default=DEFAULTS.TOD_PROPERTIES_CSV_PATH,
        help="File where the property results should be stored",
    )
    parser.add_argument(
        "--properties-details-jsonl",
        type=Path,
        default=DEFAULTS.TOD_PROPERTIES_JSONL_PATH,
        help="File where the additional info for the property results should be stored",
    )
    parser.set_defaults(func=check_command)


def check_command(args: Namespace, time_tracker: TimeTracker):
    transactions_csv_path: Path = args.base_dir / args.tod_candidates_csv
    tod_check_results_file_path: Path = args.base_dir / args.results_csv
    tod_check_details_file_path: Path = args.base_dir / args.results_details_jsonl
    tod_properties_results_file_path: Path = args.base_dir / args.properties_csv
    tod_properties_details_file_path: Path = (
        args.base_dir / args.properties_details_jsonl
    )
    tod_method = args.tod_method

    checker = create_checker(args.provider)

    with time_tracker.task(("check",)):
        check(
            checker,
            transactions_csv_path,
            tod_check_results_file_path,
            tod_check_details_file_path,
            tod_method,
            args.max_workers,
            time_tracker,
        )

    with time_tracker.task(("properties",)):
        check_properties(
            checker,
            tod_check_results_file_path,
            tod_properties_results_file_path,
            tod_properties_details_file_path,
            args.max_workers,
            time_tracker,
        )


def check(
    checker_param: TodChecker,
    tod_candidates_path: Path,
    tod_check_results_path: Path,
    tod_check_details_path: Path,
    tod_method: TODMethod,
    max_workers: int,
    time_tracker: TimeTracker,
):
    global checker
    checker = checker_param
    transaction_pairs = load_tod_candidates(tod_candidates_path)

    blocks = set()
    with time_tracker.task(("check", "download transactions")):
        for tx in tqdm(set(flatten(transaction_pairs)), desc="Fetch transactions"):
            blocks.add(checker.download_data_for_transaction(tx))
    with time_tracker.task(("check", "fetch state changes")):
        for block in tqdm(blocks, desc="Fetch state changes"):
            checker.download_data_for_block(block)

    with time_tracker.task(("check", "check")):
        with open(tod_check_results_path, "w", newline="") as csv_file, open(
            tod_check_details_path, "w"
        ) as details_file:
            writer = csv.DictWriter(csv_file, ["tx_a", "tx_b", "result"])
            writer.writeheader()
            with ThreadPool(max_workers) as p:
                process_inputs = [
                    CheckArgs((tx_a, tx_b), tod_method)
                    for tx_a, tx_b in transaction_pairs
                ]
                for result in tqdm(
                    p.imap_unordered(check_candidate, process_inputs, chunksize=1),
                    total=len(process_inputs),
                    desc="Check TOD",
                ):
                    time_tracker.save_time_ms(
                        ("check", "check", result.id), result.elapsed_ms
                    )
                    tx_a, tx_b = result.id.split("_")
                    writer.writerow(
                        {
                            "tx_a": tx_a,
                            "tx_b": tx_b,
                            "result": result.result,
                        }
                    )
                    details: dict = {
                        "tx_a": tx_a,
                        "tx_b": tx_b,
                        "details": None,
                        "failure": None,
                    }
                    if result.details:
                        details["details"] = result.details.as_dict()
                    if result.result not in ("TOD", "not TOD"):
                        details["failure"] = result.result
                    details_file.write(json.dumps(details) + "\n")


def check_properties(
    checker_param: TodChecker,
    tod_check_results_path: Path,
    tod_properties_path: Path,
    tod_properties_details_path: Path,
    max_workers: int,
    time_tracker: TimeTracker,
):
    global checker
    checker = checker_param

    tods = load_tod_transactions(tod_check_results_path)

    with ThreadPool(max_workers) as p:
        with open(tod_properties_path, "w", newline="") as csv_file, open(
            tod_properties_details_path, "w"
        ) as details_file:
            writer = csv.DictWriter(
                csv_file,
                [
                    "tx_a",
                    "tx_b",
                    "attacker_gain_and_victim_loss",
                    "attacker_gain_and_victim_loss_approximation",
                    "tod_transfer",
                    "tod_amount",
                    "tod_receiver",
                    "erc20_approval",
                ],
            )
            writer.writeheader()
            process_inputs = [CheckPropertiesArgs((tx_a, tx_b)) for tx_a, tx_b in tods]
            for result in tqdm(
                p.imap_unordered(
                    check_candidate_properties, process_inputs, chunksize=1
                ),
                total=len(process_inputs),
                desc="Check properties",
            ):
                tx_a, tx_b = result.transaction_hashes
                id = f"{tx_a}_{tx_b}"
                time_tracker.save_time_ms(("properties", id), result.elapsed_ms)

                details = None
                failure = None
                if (
                    result.gain_and_loss
                    and result.gain_and_loss_approximation
                    and result.securify_tx_a
                    and result.securify_tx_b
                    and result.erc20_approval
                ):
                    writer.writerow(
                        {
                            "tx_a": tx_a,
                            "tx_b": tx_b,
                            "attacker_gain_and_victim_loss": result.gain_and_loss[
                                "properties"
                            ]["attacker_gain_and_victim_loss"],
                            "attacker_gain_and_victim_loss_approximation": result.gain_and_loss_approximation[
                                "properties"
                            ]["attacker_gain_and_victim_loss"],
                            "tod_transfer": result.securify_tx_a["properties"][
                                "TOD_Transfer"
                            ]
                            or result.securify_tx_b["properties"]["TOD_Transfer"],
                            "tod_amount": result.securify_tx_a["properties"][
                                "TOD_Amount"
                            ]
                            or result.securify_tx_b["properties"]["TOD_Amount"],
                            "tod_receiver": result.securify_tx_a["properties"][
                                "TOD_Receiver"
                            ]
                            or result.securify_tx_b["properties"]["TOD_Receiver"],
                            "erc20_approval": result.erc20_approval["properties"][
                                "approve_after_transfer"
                            ],
                        }
                    )
                    details = {
                        "tx_a": tx_a,
                        "tx_b": tx_b,
                        "gain_and_loss": result.gain_and_loss,
                        "securify_tx_a": result.securify_tx_a,
                        "securify_tx_b": result.securify_tx_b,
                        "erc20_approval": result.erc20_approval,
                    }
                if result.error:
                    failure = traceback.format_exception(result.error)
                details_obj: dict = {
                    "tx_a": tx_a,
                    "tx_b": tx_b,
                    "details": details,
                    "failure": failure,
                }
                details_file.write(json.dumps(details_obj) + "\n")


def create_checker(provider: str):
    rpc = RPC(provider, OverridesFormatter("old Erigon"))
    state_changes_fetcher = StateChangesFetcher(rpc)
    tx_block_mapper = TransactionBlockMapper(rpc)
    simulator = TransactionExecutor(rpc)
    return TodChecker(simulator, state_changes_fetcher, tx_block_mapper)


@dataclass
class CheckArgs:
    transaction_hashes: tuple[str, str]
    tod_method: TODMethod


@dataclass
class CheckResult:
    id: str
    result: TODResult
    details: TODCheckResult | None
    elapsed_ms: int


def check_candidate(args: CheckArgs):
    res = None

    with StopWatch() as stopwatch:
        global checker
        assert checker is not None
        tx_a, tx_b = args.transaction_hashes
        try:
            res = checker.check(
                tx_a,
                tx_b,
            )
            if args.tod_method == "approximation":
                result = "TOD" if res.tx_b_comparison.differences() else "not TOD"
            else:
                result = "TOD" if res.overall_comparison.differences() else "not TOD"
        except ReplayDivergedException:
            result = "replay diverged"
        except InsufficientEtherReplayException:
            result = "insufficient ether replay detected"
        except Exception as e:
            if "insufficient funds" in str(e).lower():
                result = "insufficient ether replay error"
            else:
                result = "error"

    return CheckResult(
        f"{args.transaction_hashes[0]}_{args.transaction_hashes[1]}",
        result,  # type: ignore
        res,
        stopwatch.elapsed_ms(),
    )


@dataclass
class CheckPropertiesArgs:
    transaction_hashes: tuple[str, str]


@dataclass
class CheckPropertiesResult:
    transaction_hashes: tuple[str, str]
    error: Exception | None
    gain_and_loss: GainAndLossResult | None
    gain_and_loss_approximation: GainAndLossResult | None
    securify_tx_a: SecurifyCheckResult | None
    securify_tx_b: SecurifyCheckResult | None
    erc20_approval: ERC20ApprovalCheckResult | None
    elapsed_ms: int


def check_candidate_properties(args: CheckPropertiesArgs):
    error = None
    gain_and_loss = None
    gain_and_loss_approx = None
    securify_tx_a = None
    securify_tx_b = None
    approval = None

    with StopWatch() as stopwatch:
        global checker
        assert checker is not None
        tx_a, tx_b = args.transaction_hashes
        try:
            analyzer = CurrencyChangesJSTracer()
            js_tracer, config = analyzer.get_js_tracer()
            traces = checker.js_trace_scenarios(tx_a, tx_b, js_tracer, config)
            currency_changes, events = analyzer.process_traces(traces)

            tx_a_data = checker._tx_block_mapper.get_transaction(tx_a)
            tx_b_data = checker._tx_block_mapper.get_transaction(tx_b)

            gain_and_loss_approx = check_gain_and_loss_properties(
                changes_normal=currency_changes.tx_b_normal,
                changes_reverse=currency_changes.tx_b_reverse,
                accounts={
                    "attacker_eoa": tx_a_data["from"],
                    "attacker_bot": tx_a_data["to"],
                    "victim": tx_b_data["from"],
                },
            )
            gain_and_loss = check_gain_and_loss_properties(
                changes_normal=[
                    *currency_changes.tx_a_normal,
                    *currency_changes.tx_b_normal,
                ],
                changes_reverse=[
                    *currency_changes.tx_a_reverse,
                    *currency_changes.tx_b_reverse,
                ],
                accounts={
                    "attacker_eoa": tx_a_data["from"],
                    "attacker_bot": tx_a_data["to"],
                    "victim": tx_b_data["from"],
                },
            )
            securify_tx_a = check_securify_properties(
                currency_changes.tx_a_normal, currency_changes.tx_a_reverse
            )
            securify_tx_b = check_securify_properties(
                currency_changes.tx_b_normal, currency_changes.tx_b_reverse
            )
            approval = check_erc20_approval_attack(
                events.tx_a_normal, events.tx_b_normal
            )
        except Exception as e:
            error = e

    return CheckPropertiesResult(
        args.transaction_hashes,
        error,
        gain_and_loss,
        gain_and_loss_approx,
        securify_tx_a,
        securify_tx_b,
        approval,
        stopwatch.elapsed_ms(),
    )


def load_tod_candidates(csv_path: Path) -> Sequence[tuple[str, str]]:
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return [(row["tx_a"], row["tx_b"]) for row in reader]


def load_tod_transactions(csv_path: Path) -> Sequence[tuple[str, str]]:
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return [(row["tx_a"], row["tx_b"]) for row in reader if row["result"] == "TOD"]


def flatten(nested_list: Iterable[Iterable]) -> list:
    return [element for sublist in nested_list for element in sublist]
