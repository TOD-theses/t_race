from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from importlib.metadata import version
import json
from multiprocessing import Pool
import os
from pathlib import Path
import traceback
from typing import Iterable, Sequence

from tqdm import tqdm
from traces_analyzer.loader.directory_loader import DirectoryLoader
from traces_analyzer.loader.event_parser import VmTraceEventsParser
from traces_analyzer.loader.loader import PotentialAttack
from traces_analyzer.cli import (
    HexString,
    Evaluation,
    TODSourceFeatureExtractor,
    InstructionDifferencesFeatureExtractor,
    InstructionUsagesFeatureExtractor,
    InstructionLocationsGrouperFeatureExtractor,
    CurrencyChangesFeatureExtractor,
    SingleToDoubleInstructionFeatureExtractor,
    parse_transaction,
    TransactionParsingInfo,
    RunInfo,
    FeatureExtractionRunner,
    SecurifyPropertiesEvaluation,
    FinancialGainLossEvaluation,
    OverallPropertiesEvaluation,
    CALL,
    TraceEvent,
)

from t_race.timing.stopwatch import StopWatch
from t_race.timing.time_tracker import TimeTracker


def init_parser_analyze(parser: ArgumentParser):
    parser.add_argument(
        "--version",
        action="version",
        version="traces_analyzer " + version("traces_analyzer"),
    )
    parser.add_argument(
        "--traces-path",
        type=Path,
        default=Path("traces"),
        help="Directory that contains the traces that should be analyzed",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("results"),
        help="Directory where the analysis results should be stored",
    )
    parser.set_defaults(func=analyze_command)


def analyze_command(args: Namespace, time_tracker: TimeTracker):
    traces_dir: Path = args.base_dir / args.traces_path
    results_dir: Path = args.base_dir / args.output_path

    results_dir.mkdir(exist_ok=True)

    trace_dirs = get_trace_dirs(traces_dir)
    process_inputs = [AnalyzeArgs(path, results_dir) for path in trace_dirs]

    with time_tracker.task(("analyze",)):
        with Pool(args.max_workers) as p:
            for result in tqdm(
                p.imap_unordered(analyze, process_inputs, chunksize=1),
                total=len(process_inputs),
            ):
                time_tracker.save_time_ms(("analyze", result.id), result.elapsed_ms)


def get_trace_dirs(traces_dir: Path) -> Sequence[Path]:
    return [Path(x.path) for x in os.scandir(traces_dir) if x.is_dir()]


@dataclass
class AnalyzeArgs:
    traces_path: Path
    results_directory: Path


@dataclass
class AnalyzeResult:
    id: str
    error: bool
    elapsed_ms: int


def analyze(args: AnalyzeArgs):
    error = False
    id = "exception"

    with StopWatch() as stopwatch:
        with DirectoryLoader(args.traces_path, VmTraceEventsParser()) as bundle:
            id = bundle.id
            out_path = args.results_directory / f"{bundle.id}.json"
            try:
                evaluations = analyze_attack(bundle)
                save_evaluations(
                    [evaluations.overall], args.results_directory / f"{bundle.id}.json"
                )
            except Exception:
                error = True
                msg = traceback.format_exc()
                with open(out_path, "w") as f:
                    json.dump({"exception": msg}, f)

    return AnalyzeResult(id, error, stopwatch.elapsed_ms())


@dataclass
class AttackEvaluations:
    overall: OverallPropertiesEvaluation
    tx_a: tuple[SecurifyPropertiesEvaluation, FinancialGainLossEvaluation]
    tx_b: tuple[SecurifyPropertiesEvaluation, FinancialGainLossEvaluation]


def analyze_attack(bundle: PotentialAttack) -> AttackEvaluations:
    try:
        sec_a, gain_loss_a = compare_traces(
            bundle.tx_a.caller,
            bundle.tx_a.to,
            bundle.tx_a.calldata,
            bundle.tx_a.value,
            (bundle.tx_a.events_normal, bundle.tx_a.events_reverse),
        )
        sec_b, gain_loss_b = compare_traces(
            bundle.tx_b.caller,
            bundle.tx_b.to,
            bundle.tx_b.calldata,
            bundle.tx_b.value,
            (bundle.tx_b.events_normal, bundle.tx_b.events_reverse),
        )
        overall_properties = OverallPropertiesEvaluation(
            attackers=(bundle.tx_a.caller, bundle.tx_a.to),
            victim=bundle.tx_b.caller,
            securify_properties_evaluations=(sec_a, sec_b),
            financial_gain_loss_evaluations=(gain_loss_a, gain_loss_b),
        )
        return AttackEvaluations(
            overall=overall_properties,
            tx_a=(sec_a, gain_loss_a),
            tx_b=(sec_b, gain_loss_b),
        )

    except Exception:
        raise Exception(f"Could not analyze traces for {bundle.id}")


def compare_traces(
    sender: HexString,
    to: HexString,
    calldata: HexString,
    value: HexString,
    traces: tuple[Iterable[TraceEvent], Iterable[TraceEvent]],
) -> tuple[SecurifyPropertiesEvaluation, FinancialGainLossEvaluation]:
    tod_source_analyzer = TODSourceFeatureExtractor()
    instruction_changes_analyzer = InstructionDifferencesFeatureExtractor()
    instruction_usage_analyzers = SingleToDoubleInstructionFeatureExtractor(
        InstructionUsagesFeatureExtractor(), InstructionUsagesFeatureExtractor()
    )
    currency_changes_analyzer = SingleToDoubleInstructionFeatureExtractor(
        CurrencyChangesFeatureExtractor(), CurrencyChangesFeatureExtractor()
    )
    calls_grouper = SingleToDoubleInstructionFeatureExtractor(
        InstructionLocationsGrouperFeatureExtractor([CALL.opcode]),
        InstructionLocationsGrouperFeatureExtractor([CALL.opcode]),
    )

    transaction_one = parse_transaction(
        TransactionParsingInfo(sender, to, calldata, value), traces[0]
    )
    transaction_two = parse_transaction(
        TransactionParsingInfo(sender, to, calldata, value), traces[1]
    )

    runner = FeatureExtractionRunner(
        RunInfo(
            feature_extractors=[
                tod_source_analyzer,
                instruction_changes_analyzer,
                instruction_usage_analyzers,
                currency_changes_analyzer,
                calls_grouper,
            ],
            transactions=(transaction_one, transaction_two),
        )
    )
    runner.run()

    # build_information_flow_graph(transaction_one.instructions)
    # build_information_flow_graph(transaction_two.instructions)
    evaluation_securify = SecurifyPropertiesEvaluation(
        calls_grouper.normal.instruction_groups,  # type: ignore
        calls_grouper.reverse.instruction_groups,  # type: ignore
    )
    evaluation_gain_loss = FinancialGainLossEvaluation(
        currency_changes_analyzer.normal.currency_changes,
        currency_changes_analyzer.reverse.currency_changes,
    )

    return evaluation_securify, evaluation_gain_loss


def save_evaluations(evaluations: list[Evaluation], path: Path):
    reports = {}

    for evaluation in evaluations:
        dict_report = evaluation.dict_report()
        reports[dict_report["evaluation_type"]] = dict_report["report"]

    path.write_text(json.dumps(reports, indent=2))
