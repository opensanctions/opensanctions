import csv
import json
from pathlib import Path

import click
import yaml

from clean import PROGRAM_PATH
from compare import compare_single_entity
from example_data import EXAMPLES_PATH
from optimise import LEVELS, optimise_single_entity

LEVEL_OPTIONS = click.Choice(LEVELS, case_sensitive=False)

IN_PATH = click.Path(exists=True, path_type=Path)
OUT_PATH = click.Path(path_type=Path)


class IndentedListDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):  # type: ignore
        return super().increase_indent(flow, indentless=False)


@click.group(help="Zavod DSPy optimisation and evaluation tools")
def cli(debug: bool = False) -> None:
    pass


@cli.command("optimise", help="Optimise the name cleaning prompt")
@click.argument("examples_path", type=IN_PATH, default=EXAMPLES_PATH)
@click.argument("program_path", type=OUT_PATH, default=PROGRAM_PATH)
@click.option("--level", type=str, default="heavy", help="Optimisation level")
def optimise(
    examples_path: Path = EXAMPLES_PATH,
    program_path: Path = PROGRAM_PATH,
    level: str = "heavy",
) -> None:
    optimise_single_entity(examples_path, program_path, level=level)


@cli.command("compare", help="Evaluate the optimised prompt on held-out examples")
@click.argument("output_path", type=OUT_PATH)
@click.argument("examples_path", type=IN_PATH, default=EXAMPLES_PATH)
def compare(output_path: Path, examples_path: Path = EXAMPLES_PATH) -> None:
    compare_single_entity(examples_path, output_path)


@cli.command("dump-examples")
@click.argument("input_path", type=IN_PATH)
@click.argument("output_path", type=OUT_PATH)
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
