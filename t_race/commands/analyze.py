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
from traces_analyzer.loader.loader import PotentialAttack
from traces_analyzer.cli import (
    HexString,
    Evaluation,
    TODSourceEvaluation,
    TODSourceFeatureExtractor,
    InstructionDifferencesFeatureExtractor,
    InstructionUsagesFeatureExtractor,
    SingleToDoubleInstructionFeatureExtractor,
    parse_transaction,
    parse_events,
    TransactionParsingInfo,
    RunInfo,
    FeatureExtractionRunner,
    build_information_flow_graph,
    InstructionUsageEvaluation,
    InstructionDifferencesEvaluation,
    CALL,
    STATICCALL,
)


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


def analyze_command(args: Namespace):
    traces_dir: Path = args.base_dir / args.traces_path
    results_dir: Path = args.base_dir / args.output_path

    results_dir.mkdir(exist_ok=True)

    trace_dirs = get_trace_dirs(traces_dir)
    process_inputs = [AnalyzeArgs(path, results_dir) for path in trace_dirs]

    with Pool(args.max_workers) as p:
        for _ in tqdm(
            p.imap_unordered(analyze, process_inputs, chunksize=1),
            total=len(process_inputs),
        ):
            pass


def get_trace_dirs(traces_dir: Path) -> Sequence[Path]:
    return [Path(x.path) for x in os.scandir(traces_dir) if x.is_dir()]


@dataclass
class AnalyzeArgs:
    traces_path: Path
    results_directory: Path


def analyze(args: AnalyzeArgs):
    with DirectoryLoader(args.traces_path) as bundle:
        out_path = args.results_directory / f"{bundle.id}.json"
        try:
            evaluations = analyze_attack(bundle)
            save_evaluations(evaluations, args.results_directory / f"{bundle.id}.json")
        except Exception:
            msg = traceback.format_exc()
            with open(out_path, "w") as f:
                json.dump({"exception": msg}, f)


def analyze_attack(bundle: PotentialAttack):
    try:
        return compare_traces(
            bundle.tx_victim.caller,
            bundle.tx_victim.to,
            bundle.tx_victim.calldata,
            bundle.tx_victim.value,
            (bundle.tx_victim.trace_actual, bundle.tx_victim.trace_reverse),
        )
    except Exception:
        raise Exception(f"Could not analyze traces for {bundle.id}")


def compare_traces(
    sender: HexString,
    to: HexString,
    calldata: HexString,
    value: HexString,
    traces: tuple[Iterable[str], Iterable[str]],
) -> list[Evaluation]:
    tod_source_analyzer = TODSourceFeatureExtractor()
    instruction_changes_analyzer = InstructionDifferencesFeatureExtractor()
    instruction_usage_analyzers = SingleToDoubleInstructionFeatureExtractor(
        InstructionUsagesFeatureExtractor(), InstructionUsagesFeatureExtractor()
    )

    transaction_one = parse_transaction(
        TransactionParsingInfo(sender, to, calldata, value), parse_events(traces[0])
    )
    transaction_two = parse_transaction(
        TransactionParsingInfo(sender, to, calldata, value), parse_events(traces[1])
    )

    runner = FeatureExtractionRunner(
        RunInfo(
            feature_extractors=[
                tod_source_analyzer,
                instruction_changes_analyzer,
                instruction_usage_analyzers,
            ],
            transactions=(transaction_one, transaction_two),
        )
    )
    runner.run()

    build_information_flow_graph(transaction_one.instructions)
    build_information_flow_graph(transaction_two.instructions)

    evaluations: list[Evaluation] = [
        TODSourceEvaluation(tod_source_analyzer.get_tod_source()),
        InstructionDifferencesEvaluation(
            occurrence_changes=instruction_changes_analyzer.get_instructions_only_executed_by_one_trace(),
            input_changes=instruction_changes_analyzer.get_instructions_with_different_inputs(),
        ),
        InstructionUsageEvaluation(
            instruction_usage_analyzers.one.get_used_opcodes_per_contract(),
            instruction_usage_analyzers.two.get_used_opcodes_per_contract(),
            filter_opcodes=[CALL.opcode, STATICCALL.opcode],
        ),
    ]

    return evaluations


def save_evaluations(evaluations: list[Evaluation], path: Path):
    reports = {}

    for evaluation in evaluations:
        dict_report = evaluation.dict_report()
        reports[dict_report["evaluation_type"]] = dict_report["report"]

    path.write_text(json.dumps(reports, indent=2))
