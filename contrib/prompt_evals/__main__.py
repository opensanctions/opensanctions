import json
import logging
from datetime import datetime
from pathlib import Path

import click
from zavod.extract.llm import DEFAULT_MODEL
from zavod.logs import configure_logging

from contrib.prompt_evals.crawler import (
    CrawlerPrompt,
    fixtures_path,
    load_dataset,
    make_context,
)
from contrib.prompt_evals.evaluate import compare, run, save_summary, summarise
from contrib.prompt_evals.fixtures import export_fixtures


@click.group()
def cli() -> None:
    """Evaluate LLM extraction prompts against human-accepted review fixtures."""
    configure_logging(level=logging.WARNING)


@cli.command()
@click.argument("dataset_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--response-type",
    required=True,
    help="Crawler module attribute naming the pydantic response model",
)
@click.option(
    "--since",
    type=click.DateTime(),
    default=None,
    help="Only reviews accepted at or after this time",
)
@click.option(
    "--before",
    type=click.DateTime(),
    default=None,
    help="Only reviews accepted before this time",
)
def export(
    dataset_path: Path,
    response_type: str,
    since: datetime | None,
    before: datetime | None,
) -> None:
    """Add accepted reviews from the review database to the dataset's fixtures."""
    dataset = load_dataset(dataset_path)
    crawler = CrawlerPrompt(dataset, response_type)
    context = make_context(dataset)
    added, skipped = export_fixtures(
        context, fixtures_path(dataset), crawler.response_type, since, before
    )
    context.close()
    click.echo(
        f"Added {added} cases, skipped {skipped} already present in {fixtures_path(dataset)}"
    )


@cli.command()
@click.argument("dataset_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--response-type",
    required=True,
    help="Crawler module attribute naming the pydantic response model",
)
@click.option(
    "--crawler-file",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Evaluate the prompt in this crawler module instead of the dataset's entry point",
)
@click.option("--case", "names", multiple=True, help="Only run the named case(s)")
@click.option(
    "--edited-only",
    is_flag=True,
    help="Only cases where the reviewer edited the extraction",
)
@click.option("--limit", type=int, default=None)
@click.option("--model", default=DEFAULT_MODEL, show_default=True)
@click.option("--concurrency", default=4, show_default=True)
@click.option(
    "--save",
    type=click.Path(path_type=Path),
    default=None,
    help="Write a JSON summary of the results, for use as a baseline",
)
@click.option(
    "--baseline",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Compare against a saved summary",
)
@click.option(
    "--show-output", is_flag=True, help="Include model output in the report table"
)
def evaluate(
    dataset_path: Path,
    response_type: str,
    crawler_file: Path | None,
    names: tuple[str, ...],
    edited_only: bool,
    limit: int | None,
    model: str,
    concurrency: int,
    save: Path | None,
    baseline: Path | None,
    show_output: bool,
    fresh: bool,
) -> None:
    """Run the crawler's current prompt against its fixtures and report."""
    dataset = load_dataset(dataset_path)
    crawler = CrawlerPrompt(dataset, response_type, crawler_file)
    report = run(
        dataset,
        crawler,
        fixtures_path(dataset),
        list(names),
        edited_only,
        limit,
        model,
        concurrency,
        fresh,
    )
    report.print(
        include_reasons=True,
        include_output=show_output,
        include_durations=False,
        width=200,
    )
    if save is not None:
        save_summary(report, save)
        click.echo(f"Saved summary to {save}")
    if baseline is not None:
        lines = compare(summarise(report), json.loads(baseline.read_text()))
        click.echo(f"\nChanges against baseline {baseline}:")
        for line in lines or ["  none"]:
            click.echo(f"  {line}")


if __name__ == "__main__":
    cli()
