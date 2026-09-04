# Load leveldb before importing dspy to prevent
# src/tcmalloc.cc:309] Attempt to free invalid pointer 0x600002f2ede0
# on exit. See: https://github.com/google/leveldb/issues/634
import plyvel  #  type: ignore  # isort:skip  # noqa: F401

import csv
import json
from pathlib import Path

import click
import yaml
from followthemoney.cli.util import InPath, OutPath

from zavod.extract.names.clean import SINGLE_ENTITY_PROGRAM_PATH
from zavod.extract.names.dspy.compare import (
    compare_single_entity,
    rescore_single_entity,
)
from zavod.extract.names.dspy.example_data import EXAMPLES_PATH
from zavod.extract.names.dspy.optimise import LEVELS, optimise_single_entity
from zavod.extract.names.dspy.review_examples import (
    existing_identities,
    load_candidates,
    write_candidates,
)

LEVEL_OPTIONS = click.Choice(LEVELS, case_sensitive=False)


class IndentedListDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):  # type: ignore
        return super().increase_indent(flow, indentless=False)


@click.group(help="Zavod DSPy optimisation and evaluation tools")
def cli(debug: bool = False) -> None:
    pass


@cli.command("optimise", help="Crawl a specific dataset")
@click.argument("examples_path", type=InPath, default=EXAMPLES_PATH)
@click.argument("program_path", type=OutPath, default=SINGLE_ENTITY_PROGRAM_PATH)
@click.option("--level", type=str, default="heavy", help="Optimisation level")
def optimise(
    examples_path: Path = EXAMPLES_PATH,
    program_path: Path = SINGLE_ENTITY_PROGRAM_PATH,
    level: str = "heavy",
) -> None:
    optimise_single_entity(examples_path, program_path, level=level)


@cli.command("compare", help="Compare DSPy module against direct LLM calls")
@click.argument("output_path", type=OutPath)
@click.argument("examples_path", type=InPath, default=EXAMPLES_PATH)
def compare(output_path: Path, examples_path: Path = EXAMPLES_PATH) -> None:
    compare_single_entity(examples_path, output_path)


@cli.command("rescore", help="Re-score a compare output file with the current metric")
@click.argument("results_path", type=InPath)
def rescore(results_path: Path) -> None:
    rescore_single_entity(results_path)


@cli.command("review-examples")
@click.argument("output_path", type=OutPath)
@click.argument("report_path", type=OutPath)
@click.option(
    "--origin-like",
    default="gpt-%",
    help="SQL LIKE pattern for the review origin, e.g. 'gpt-%' for LLM-cleaned reviews or '%' for all.",
)
@click.option("--examples-path", type=InPath, default=EXAMPLES_PATH)
def review_examples(
    output_path: Path,
    report_path: Path,
    origin_like: str,
    examples_path: Path = EXAMPLES_PATH,
) -> None:
    """
    Export accepted name reviews from the review database as candidate DSPy
    examples (YAML, grouped by dataset, with the LLM's output and the kind of
    reviewer edit in comments) plus a Markdown report of edit kinds. Examples
    already in the examples file are skipped.
    """
    candidates = load_candidates(origin_like)
    write_candidates(
        candidates, output_path, report_path, existing_identities(examples_path)
    )
    print(f"Wrote {output_path} and {report_path}")


@cli.command("dump-examples")
@click.argument("input_path", type=InPath)
@click.argument("output_path", type=OutPath)
def dump_examples(input_path: Path, output_path: Path) -> None:
    """
    Takes a Data Reviews CSV dump and exports the source and extracted data
    as a YAML file for use as DSPy example data.
    """
    with input_path.open() as f:
        reader = csv.DictReader(f)
        reviews = list(reader)

    examples = []
    for review in reviews:
        example = json.loads(review["source_value"])
        for key, value in json.loads(review["extracted_data"]).items():
            if value:
                example[key] = value
        examples.append(example)

    with output_path.open("w") as f:
        yaml.dump(
            examples,
            f,
            Dumper=IndentedListDumper,
            default_flow_style=False,
            sort_keys=False,
        )


if __name__ == "__main__":
    cli()
