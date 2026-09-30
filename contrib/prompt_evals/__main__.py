import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import click
from zavod.logs import configure_logging

from contrib.prompt_evals.coverage import coverage_counts, select_cases, tag_cases
from contrib.prompt_evals.crawler import (
    CrawlerPrompt,
    fixtures_path,
    load_dataset,
    make_context,
)
from contrib.prompt_evals.evaluate import (
    compare,
    run,
    save_summary,
    stability_lines,
    summarise,
)
from contrib.prompt_evals.fixtures import (
    SOURCES_DIR,
    export_fixtures,
    load_fixtures,
    save_fixtures,
)
from contrib.prompt_evals.models import FixtureDataset
from contrib.prompt_evals.render import render_dataset


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
@click.option(
    "--model",
    default=None,
    help="Model to run; defaults to the crawler's LLM_MODEL, else zavod's default",
)
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
@click.option(
    "--fresh",
    is_flag=True,
    help="Ignore cached model responses, to measure run-to-run variation of an unchanged prompt",
)
@click.option(
    "--repeat",
    default=1,
    show_default=True,
    help="Run every case this many times; scores are averaged and assertions become pass rates",
)
@click.option(
    "--outputs",
    type=click.Path(path_type=Path),
    default=None,
    help="Write the model output for every case to this JSON file, keyed by case name",
)
def evaluate(
    dataset_path: Path,
    response_type: str,
    crawler_file: Path | None,
    names: tuple[str, ...],
    edited_only: bool,
    limit: int | None,
    model: str | None,
    concurrency: int,
    save: Path | None,
    baseline: Path | None,
    show_output: bool,
    fresh: bool,
    repeat: int,
    outputs: Path | None,
) -> None:
    """Run the crawler's current prompt against its fixtures and report."""
    dataset = load_dataset(dataset_path)
    crawler = CrawlerPrompt(dataset, response_type, crawler_file)
    model = model or crawler.model
    collected: dict[str, Any] = {}
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
        collected if outputs is not None else None,
        repeat,
    )
    if outputs is not None:
        outputs.parent.mkdir(parents=True, exist_ok=True)
        outputs.write_text(json.dumps(collected, indent=2, ensure_ascii=False))
    click.echo(f"Model: {model}")
    summary = summarise(report)
    if repeat == 1:
        report.print(
            include_reasons=True,
            include_output=show_output,
            include_durations=False,
            width=200,
        )
    else:
        click.echo(f"\n{repeat} runs per case. Metrics that varied between runs:")
        for line in stability_lines(summary) or ["  none"]:
            click.echo(f"  {line}")
        click.echo("\nAverages over all runs:")
        totals: dict[str, list[float]] = {}
        for case in summary["cases"].values():
            for metric, value in {
                **case.get("scores", {}),
                **case.get("assertions", {}),
            }.items():
                totals.setdefault(metric, []).append(value)
        for metric, values in totals.items():
            click.echo(f"  {metric}: {sum(values) / len(values):.3f}")
    if save is not None:
        save_summary(report, save)
        click.echo(f"Saved summary to {save}")
    if baseline is not None:
        lines = compare(summary, json.loads(baseline.read_text()))
        click.echo(f"\nChanges against baseline {baseline}:")
        for line in lines or ["  none"]:
            click.echo(f"  {line}")


@cli.command()
@click.argument("dataset_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--per-case", is_flag=True, help="Also list the rules each case exercises"
)
def coverage(dataset_path: Path, per_case: bool) -> None:
    """Count how many fixture cases exercise each rule."""
    dataset = load_dataset(dataset_path)
    fixtures_file = fixtures_path(dataset)
    fixtures = load_fixtures(fixtures_file)
    tags_by_case = tag_cases(fixtures, fixtures_file.parent)
    click.echo(f"{len(fixtures.cases)} cases")
    for tag, count in sorted(coverage_counts(tags_by_case).items()):
        click.echo(f"{count:4d}  {tag}")
    if per_case:
        click.echo()
        for name, tags in tags_by_case.items():
            click.echo(f"{name}: {', '.join(sorted(tags))}")


@cli.command()
@click.argument("dataset_path", type=click.Path(exists=True, path_type=Path))
@click.option("--target", default=3, show_default=True, help="Cases wanted per rule")
@click.option("--keep", multiple=True, help="Case names to include regardless")
@click.option(
    "--max-items",
    type=int,
    default=None,
    help="Skip cases with more extracted items than this unless kept; they cost the most to review",
)
@click.option(
    "--write",
    is_flag=True,
    help="Replace the fixtures file with the selection and prune unused sources",
)
def select(
    dataset_path: Path,
    target: int,
    keep: tuple[str, ...],
    max_items: int | None,
    write: bool,
) -> None:
    """Pick the smallest set of cases that still covers every rule `target` times."""
    dataset = load_dataset(dataset_path)
    fixtures_file = fixtures_path(dataset)
    fixtures = load_fixtures(fixtures_file)
    tags_by_case = tag_cases(fixtures, fixtures_file.parent)
    chosen = select_cases(fixtures, tags_by_case, target, set(keep), max_items)
    click.echo(f"Selected {len(chosen)} of {len(fixtures.cases)} cases:")
    for name in chosen:
        click.echo(f"  {name}: {', '.join(sorted(tags_by_case[name]))}")
    if write:
        cases = [c for c in fixtures.cases if c.name in chosen]
        save_fixtures(
            fixtures_file,
            FixtureDataset(
                name=fixtures.name, cases=cases, evaluators=fixtures.evaluators
            ),
        )
        referenced = {c.inputs.source_file for c in cases}
        pruned = 0
        for path in (fixtures_file.parent / SOURCES_DIR).iterdir():
            if f"{SOURCES_DIR}/{path.name}" not in referenced:
                path.unlink()
                pruned += 1
        click.echo(
            f"Wrote {len(cases)} cases to {fixtures_file}, pruned {pruned} unused sources"
        )


@cli.command()
@click.argument("dataset_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=None,
    help="Output HTML file (default data/prompt_evals/<dataset>_fixtures.html)",
)
@click.option(
    "--ide-link",
    default=None,
    help="Link template to open a case in an editor, e.g. 'vscode://file/{path}:{line}'",
)
def render(dataset_path: Path, out: Path | None, ide_link: str | None) -> None:
    """Write a static HTML page showing every fixture's source beside its accepted extraction."""
    dataset = load_dataset(dataset_path)
    fixtures_file = fixtures_path(dataset)
    fixtures = load_fixtures(fixtures_file)
    tags_by_case = tag_cases(fixtures, fixtures_file.parent)
    page = render_dataset(fixtures, fixtures_file, tags_by_case, ide_link)
    if out is None:
        out = Path("data/prompt_evals") / f"{dataset.name}_fixtures.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(page)
    click.echo(f"Wrote {out} ({out.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    cli()
