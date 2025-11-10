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

from zavod.shed.names.dspy.compare import compare_single_entity
from zavod.shed.names.dspy.example_data import EXAMPLES_PATH
from zavod.shed.names.dspy.optimise import LEVELS, optimise_single_entity

SINGLE_ENTITY = "single_entity"
MODULES = click.Choice([SINGLE_ENTITY], case_sensitive=False)
LEVEL_OPTIONS = click.Choice(LEVELS, case_sensitive=False)


@click.group(help="Zavod DSPy optimisation and evaluation tools")
def cli(debug: bool = False) -> None:
    pass


@cli.command("optimise", help="Crawl a specific dataset")
@click.argument("name", type=MODULES)
@click.option(
    "--examples-path", type=InPath, default=EXAMPLES_PATH, help="Path to examples file"
)
@click.option("--level", type=str, default="heavy", help="Optimisation level")
def optimise(
    name: str, examples_path: Path = EXAMPLES_PATH, level: str = "heavy"
) -> None:
    if name == SINGLE_ENTITY:
        optimise_single_entity(examples_path, level=level)
    else:
        raise ValueError(f"Unknown optimisation target: {name}")


@cli.command("compare", help="Compare DSPy module against direct LLM calls")
@click.argument("name", type=MODULES)
@click.argument("output_path", type=OutPath)
@click.option(
    "--examples-path", type=InPath, default=EXAMPLES_PATH, help="Path to examples file"
)
def compare(name: str, output_path: Path, examples_path: Path = EXAMPLES_PATH) -> None:
    if name == SINGLE_ENTITY:
        compare_single_entity(examples_path, output_path)
    else:
        raise ValueError(f"Unknown comparison target: {name}")


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
        example = {"string": review["source_value"]}
        for key, value in json.loads(review["extracted_data"]).items():
            if value:
                example[key] = value
        examples.append(example)

    with output_path.open("w") as f:
        yaml.dump(examples, f, sort_keys=False)


if __name__ == "__main__":
    cli()
