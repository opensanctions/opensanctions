"""Run the inspection eval suite against frozen HTML fixtures.

Datasets live in datasets/ as .yml and are validated from their Pydantic
models when loaded; fixtures are fictional pages structurally equivalent to
real captures. Unlike the pytest suite, this hits a real model and costs tokens —
run it deliberately, with the zavod virtualenv active:

    python -m zavod.shed.funes.evals.run [--dataset PATH] [--model NAME]
        [--case NAME ...]

``--model`` defaults to $FUNES_MODEL; the thinking level always comes from
$FUNES_THINKING, so evals judge the agent exactly as the runner configures it.
Exit status is nonzero when a case fails, raises, or an evaluator crashes.
"""

import argparse
from functools import partial
from pathlib import Path
from typing import cast

from pydantic_evals import Dataset
from pydantic_ai.settings import ThinkingEffort

from zavod.shed.funes.evals.evaluators import CUSTOM_EVALUATOR_TYPES
from zavod.shed.funes.evals.models import FixtureInput
from zavod.shed.funes.extract import PageResult
from zavod.shed.funes.evals.task import extract
from zavod import settings
from zavod.exc import ConfigurationException

DEFAULT_DATASET = Path(__file__).parent / "datasets" / "extraction.yml"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset", type=Path, default=DEFAULT_DATASET, help="dataset YAML file"
    )
    parser.add_argument(
        "--model",
        default=None,
        help="model id (provider:model); defaults to $FUNES_MODEL",
    )
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        metavar="NAME",
        help="run only this case; may be supplied more than once",
    )
    args = parser.parse_args()
    model = args.model or settings.FUNES_MODEL
    if model is None:
        raise ConfigurationException("No --model given and no $FUNES_MODEL provided.")
    if settings.FUNES_THINKING is None:
        raise ConfigurationException("No $FUNES_THINKING provided.")

    dataset = Dataset[FixtureInput, PageResult].from_file(
        args.dataset, custom_evaluator_types=CUSTOM_EVALUATOR_TYPES
    )
    if args.case:
        requested = set(args.case)
        available = {case.name for case in dataset.cases}
        if unknown := requested - available:
            parser.error("unknown case(s): " + ", ".join(sorted(unknown)))
        dataset.cases = [case for case in dataset.cases if case.name in requested]
    report = dataset.evaluate_sync(
        partial(
            extract,
            model=model,
            thinking=cast(ThinkingEffort, settings.FUNES_THINKING),
        ),
        name=f"inspection:{model}",
        progress=False,
    )
    report.print(
        include_input=True,
        include_output=True,
        include_durations=True,
        include_reasons=True,
    )

    failed = (
        bool(report.failures or report.report_evaluator_failures)
        or any(case.evaluator_failures for case in report.cases)
        or any(
            result.value is False
            for case in report.cases
            for result in case.assertions.values()
        )
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
