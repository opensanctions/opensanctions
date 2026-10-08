import asyncio
import hashlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import click
import yaml  # type: ignore[import-untyped]
from genai_prices import UpdatePrices
from pydantic_ai import Agent, AgentRunResult, NativeOutput
from pydantic_ai.models.anthropic import AnthropicModelSettings
from pydantic_ai.models.openai import OpenAIResponsesModelSettings
from pydantic_ai.native_tools import WebSearchTool
from pydantic_ai.capabilities import NativeTool
from pydantic_ai.messages import ModelResponse
from pydantic_ai.usage import RequestUsage
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
from zavod.meta import Dataset, load_directory_catalog

from models import (
    DATA_DIR,
    DATASETS_DIR,
    AnnotationRecord,
    AnnotatorResponse,
    DatasetConfig,
    Item,
    PrimaryAnnotation,
    Review,
    VetoResponse,
    read_items,
    write_jsonl,
)
from ui import STATE_STYLES, ListApp, record_state

PRIMARY_MODEL = "anthropic:claude-opus-5-5"
REVIEW_MODEL = "openai:gpt-6.1-sol"
CODEBOOK = (Path(__file__).parent / "codebook.md").read_text()
WEB_SEARCH_MAX_USES = 5

ANNOTATOR_PROMPT = f"""Classify the position label below by government level, role and
seniority, following the codebook. The dataset, countries and subnational areas
describe where the label was published. Use them as context for the title. A
dataset note, if given, states facts about all positions in the dataset: treat it as
evidence.

Assume all enterprises encountered in our datasets meet the SOE criteria.

For each undecided dimension, return 'undecided' for level or seniority, or no roles
for role. Do not guess.

Web search is available, up to {WEB_SEARCH_MAX_USES} searches: facts found through web
research count as evidence. Use it to check facts about the office or its
organization, for example whether a body is public or which government level it
belongs to.

In key_evidence, cite only facts stated in the label and its context, or found
through web research. For each web fact, give the source URL and quote the passage
that states it verbatim. Do not present inferences as facts.

<codebook>
{CODEBOOK}
</codebook>
"""

REVIEWER_PROMPT = f"""An annotator classified a position label by government level, role
and seniority, using the codebook below. Re-check the annotation against the label.

Assume all enterprises encountered in our datasets meet the SOE criteria.

Veto — escalating the label to a human — only if the annotation itself is unsafe: it
contradicts a codebook rule, it ignores information in the label or its context, or it
assigns a value that the evidence does not support where the codebook requires
'undecided'. A misstated detail in the reasoning is not a reason to veto if the
annotation still holds: note it in your reasoning and approve.

Seniority is often guesswork, so only veto if the evidence contradicts the annotation.

The annotator could search the web; you cannot. You may use the evidence in
key_evidence, including web facts with a source URL and a verbatim quote. A dataset
note, if given, states facts about all positions in the dataset: treat it as evidence.

<codebook>
{CODEBOOK}
</codebook>
"""


@dataclass(frozen=True)
class DatasetContext:
    dataset: Dataset
    config: DatasetConfig


def render_item(item: Item, context: DatasetContext) -> str:
    model = context.dataset.model
    addendum = context.config.prompt_addendum
    return (
        "<position>\n"
        f"Title: {item.caption}\n"
        f"Countries: {', '.join(item.countries) or '-'}\n"
        f"Subnational areas: {'; '.join(item.subnational_areas) or '-'}\n"
        "</position>\n\n"
        "<dataset>\n"
        f"Name: {context.dataset.name}\n"
        f"Title: {model.title}\n"
        f"Summary: {(model.summary or '-').strip()}\n"
        f"Description:\n{(model.description or '-').strip()}\n"
        "</dataset>"
        + (
            f"\n\n<dataset_note>\n{addendum.strip()}\n</dataset_note>"
            if addendum
            else ""
        )
    )


def load_dataset_configs() -> dict[str, DatasetConfig]:
    configs: dict[str, DatasetConfig] = {}
    for path in sorted(DATASETS_DIR.glob("**/*.yml")):
        if path.stem in configs:
            raise click.ClickException(f"Duplicate dataset config: {path}")
        with path.open() as fh:
            configs[path.stem] = DatasetConfig.model_validate(yaml.safe_load(fh))
    return configs


def load_datasets(items: list[Item]) -> dict[str, DatasetContext]:
    catalog = load_directory_catalog()
    configs = load_dataset_configs()
    # A config named after no known dataset is a typo that would silently do nothing.
    for name in configs:
        catalog.require(name)
    return {
        name: DatasetContext(
            dataset=catalog.require(name),
            config=configs.get(name, DatasetConfig()),
        )
        for name in sorted({i.dataset for i in items})
    }


def load_records(items: list[Item], output: Path) -> list[AnnotationRecord]:
    """Resume from the output file, provided it covers exactly the input items."""
    if len({item.id for item in items}) != len(items):
        raise click.ClickException("The input file contains duplicate item IDs.")
    if not output.exists():
        return [AnnotationRecord(item=item) for item in items]
    with output.open() as fh:
        records = [
            AnnotationRecord.model_validate_json(line) for line in fh if line.strip()
        ]
    if {record.item for record in records} != set(items) or len(records) != len(items):
        raise click.ClickException(
            f"{output} holds a different set of items than the input. "
            "Use another --output, or delete the file to start again."
        )
    return records


class Saver:
    """Rewrite the output at most every few seconds, so large inputs stay fast."""

    def __init__(
        self, path: Path, records: list[AnnotationRecord], interval: float = 5.0
    ) -> None:
        self.path = path
        self.records = records
        self.interval = interval
        self.last_save = 0.0

    def save(self, force: bool = False) -> None:
        if force or time.monotonic() - self.last_save >= self.interval:
            write_jsonl(self.path, self.records)
            self.last_save = time.monotonic()


def context_hash(rendered: str) -> str:
    return hashlib.sha256(rendered.encode()).hexdigest()


def current_primary(
    record: AnnotationRecord, rendered: str
) -> PrimaryAnnotation | None:
    """The latest primary annotation, if it comes from the configured model and
    saw the item in its current context."""
    primary = record.latest_primary()
    if primary is None:
        return None
    if primary.model != PRIMARY_MODEL or primary.context_hash != context_hash(rendered):
        return None
    return primary


def model_responses(result: AgentRunResult[Any]) -> list[ModelResponse]:
    return [m for m in result.all_messages() if isinstance(m, ModelResponse)]


async def run_models(
    records: list[AnnotationRecord],
    datasets: dict[str, DatasetContext],
    saver: Saver,
    concurrency: int,
    responses: dict[str, list[ModelResponse]],
) -> None:
    rendered = {r.id: render_item(r.item, datasets[r.item.dataset]) for r in records}
    pending = [
        r
        for r in records
        if (primary := current_primary(r, rendered[r.id])) is None
        or primary.review is None
    ]
    if not pending:
        return
    # The instructions are identical for every item, so they form the cached prefix.
    annotator = Agent(
        PRIMARY_MODEL,
        instructions=ANNOTATOR_PROMPT,
        output_type=NativeOutput(AnnotatorResponse),
        capabilities=[NativeTool(WebSearchTool(max_uses=WEB_SEARCH_MAX_USES))],
        model_settings=AnthropicModelSettings(
            thinking="medium", anthropic_cache_instructions=True
        ),
    )
    reviewer = Agent(
        REVIEW_MODEL,
        instructions=REVIEWER_PROMPT,
        output_type=NativeOutput(VetoResponse),
        model_settings=OpenAIResponsesModelSettings(
            thinking="medium", openai_prompt_cache_key="positions-review"
        ),
    )
    responses.setdefault(PRIMARY_MODEL, [])
    responses.setdefault(REVIEW_MODEL, [])
    semaphore = asyncio.Semaphore(concurrency)

    with Progress() as progress:
        task = progress.add_task("Annotating", total=len(pending))

        async def process(record: AnnotationRecord) -> None:
            async with semaphore:
                message = rendered[record.id]
                try:
                    primary = current_primary(record, message)
                    if primary is None:
                        primary_result = await annotator.run(message)
                        responses[PRIMARY_MODEL].extend(model_responses(primary_result))
                        output = primary_result.output
                        primary = PrimaryAnnotation(
                            **output.annotation.model_dump(),
                            id=uuid4(),
                            created_at=datetime.now(UTC),
                            model=PRIMARY_MODEL,
                            context_hash=context_hash(message),
                            key_evidence=output.key_evidence,
                            reasoning=output.reasoning,
                        )
                        record.annotations.append(primary)
                        saver.save()
                    if primary.review is None:
                        annotation = primary.model_dump_json(
                            include={
                                "key_evidence",
                                "reasoning",
                                "level",
                                "roles",
                                "seniority",
                            },
                            indent=2,
                        )
                        review_result = await reviewer.run(
                            f"{message}\n\n<annotation>\n{annotation}\n</annotation>"
                        )
                        responses[REVIEW_MODEL].extend(model_responses(review_result))
                        primary.review = Review(
                            created_at=datetime.now(UTC),
                            model=REVIEW_MODEL,
                            reasoning=review_result.output.reasoning,
                            veto=review_result.output.veto,
                        )
                        saver.save()
                # A failed item stays pending for the next run; it must not stop the batch.
                except Exception as exc:
                    progress.console.print(
                        f"[red]Failed[/red] {record.item.caption!r}: {exc!r}"
                    )
                progress.advance(task)

        # Parallel requests that start before a cache entry exists all miss it,
        # so one item writes the cache before the rest run.
        await process(pending[0])
        await asyncio.gather(*(process(record) for record in pending[1:]))


def print_usage(console: Console, responses: dict[str, list[ModelResponse]]) -> None:
    table = Table(title="Token usage this run")
    for column in (
        "Model",
        "Requests",
        "Input",
        "Cache read",
        "Cache write",
        "Output",
        "USD",
    ):
        table.add_column(column, justify="left" if column == "Model" else "right")
    total = Decimal(0)
    for model, model_responses in responses.items():
        usage = RequestUsage()
        cost = Decimal(0)
        unpriced = 0
        # Price each response on its own: price tiers depend on the request size.
        for response in model_responses:
            usage = usage + response.usage
            try:
                cost += response.cost().total_price
            except LookupError:
                unpriced += 1
        total += cost
        table.add_row(
            model.partition(":")[2],
            str(len(model_responses)),
            f"{usage.input_tokens:,}",
            f"{usage.cache_read_tokens:,}",
            f"{usage.cache_write_tokens:,}",
            f"{usage.output_tokens:,}",
            f"{cost:.2f}" + (f" (+{unpriced} unpriced)" if unpriced else ""),
        )
    table.add_row("Total", "", "", "", "", "", f"{total:.2f}", style="bold")
    console.print(table)


def print_summary(console: Console, records: list[AnnotationRecord]) -> None:
    states = [record_state(r) for r in records]
    table = Table(title="Annotation status")
    table.add_column("State")
    table.add_column("Items", justify="right")
    table.add_row("Golden", str(states.count("golden")))
    table.add_row("Approved by reviewer", str(states.count("approved")))
    table.add_row("Decided by human", str(states.count("human")))
    table.add_row("Vetoed, pending human", str(states.count("vetoed")))
    table.add_row("Undecided, pending human", str(states.count("undecided")))
    table.add_row("LLM calls incomplete", str(states.count("pending")))
    table.add_row("Total", str(len(records)), style="bold")
    console.print(table)


@click.group()
def cli() -> None:
    """Annotate position items and inspect the results."""


@cli.command()
@click.argument(
    "input_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Defaults to data/<input stem>.annotated.jsonl.",
)
@click.option(
    "--concurrency", default=8, show_default=True, help="Parallel LLM requests."
)
@click.option("--no-human", is_flag=True, help="Run the LLM phase only.")
def annotate(
    input_path: Path, output: Path | None, concurrency: int, no_human: bool
) -> None:
    """Annotate the items in INPUT_PATH with an LLM, an LLM vetoer and a human."""
    console = Console()
    output_path = (
        output
        if output is not None
        else DATA_DIR / f"{input_path.stem}.annotated.jsonl"
    )
    records = load_records(read_items(input_path), output_path)
    datasets = load_datasets([record.item for record in records])
    saver = Saver(output_path, records)
    responses: dict[str, list[ModelResponse]] = {}
    try:
        asyncio.run(run_models(records, datasets, saver, concurrency, responses))
        saver.save(force=True)
        if not no_human:
            ListApp(records, lambda: saver.save(force=True)).run()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[yellow]Interrupted.[/yellow]")
    finally:
        saver.save(force=True)
    print_summary(console, records)
    if any(responses.values()):
        prices = UpdatePrices()
        # A failed download leaves the prices bundled with genai-prices in use.
        try:
            prices.start(wait=30)
        except Exception as exc:
            console.print(f"[yellow]Price update failed: {exc!r}[/yellow]")
        finally:
            prices.stop()  # type: ignore[no-untyped-call]
        print_usage(console, responses)
    console.print(f"Wrote {output_path}")


@cli.command("list")
@click.argument(
    "annotated_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--state",
    type=click.Choice(list(STATE_STYLES)),
    help="Only show records in this state.",
)
def list_records(annotated_path: Path, state: str | None) -> None:
    """Browse and decide the records in an annotated file."""
    with annotated_path.open() as fh:
        records = [
            AnnotationRecord.model_validate_json(line) for line in fh if line.strip()
        ]
    shown = records
    if state is not None:
        shown = [r for r in records if record_state(r) == state]
    if not shown:
        raise click.ClickException("No records to show.")
    # Save all records, including the ones the state filter hides.
    ListApp(shown, lambda: write_jsonl(annotated_path, records)).run()


if __name__ == "__main__":
    cli()
