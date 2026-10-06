import asyncio
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import click
from genai_prices import UpdatePrices
from pydantic_ai import Agent, AgentRunResult, NativeOutput
from pydantic_ai.models.anthropic import AnthropicModelSettings
from pydantic_ai.models.openai import OpenAIResponsesModelSettings
from pydantic_ai.messages import ModelResponse
from pydantic_ai.usage import RequestUsage
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from models import (
    DATA_DIR,
    AnnotationRecord,
    AnnotatorResponse,
    Item,
    VetoResponse,
    read_items,
    write_jsonl,
)
from ui import STATE_STYLES, ListApp, ReviewApp, record_state

PRIMARY_MODEL = "anthropic:claude-opus-5-5"
REVIEW_MODEL = "openai:gpt-6.1-sol"
CODEBOOK = (Path(__file__).parent / "codebook.md").read_text()

ANNOTATOR_PROMPT = f"""Classify the position label below by government level, role and
seniority, following the codebook. The dataset name, countries and subnational areas
describe where the label was published. Use them as context for the title.

Where the codebook says a dimension is undecided, answer 'undecided' for level or
seniority, and give no roles. Do not guess.

<codebook>
{CODEBOOK}
</codebook>
"""

REVIEWER_PROMPT = f"""An annotator classified a position label by government level, role
and seniority, using the codebook below. Re-check the annotation against the label.

Veto — escalating the label to a human — only if the annotation itself is unsafe: it
contradicts a codebook rule, it ignores information in the label or its context, or it
assigns a value that the evidence does not support where the codebook requires
'undecided'. A misstated detail in the reasoning is not a reason to veto if the
annotation still holds: note it in your reasoning and approve.

<codebook>
{CODEBOOK}
</codebook>
"""


def render_item(item: Item) -> str:
    return (
        "<position>\n"
        f"Title: {item.caption}\n"
        f"Countries: {', '.join(item.countries) or '-'}\n"
        f"Subnational areas: {'; '.join(item.subnational_areas) or '-'}\n"
        f"Dataset: {item.dataset}\n"
        "</position>"
    )


def load_records(items: list[Item], output: Path) -> list[AnnotationRecord]:
    """Resume from the output file, provided it covers exactly the input items."""
    if len(set(items)) != len(items):
        raise click.ClickException("The input file contains duplicate items.")
    if not output.exists():
        return [AnnotationRecord(id=item.id, item=item) for item in items]
    with output.open() as fh:
        records = [
            AnnotationRecord.model_validate_json(line) for line in fh if line.strip()
        ]
    if {record.item for record in records} != set(items) or len(records) != len(items):
        raise click.ClickException(
            f"{output} holds a different set of items than the input. "
            "Use another --output, or delete the file to start again."
        )
    for record in records:
        if record.id != record.item.id:
            raise click.ClickException(f"Record {record.id} does not match its item.")
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


def model_responses(result: AgentRunResult[Any]) -> list[ModelResponse]:
    return [m for m in result.all_messages() if isinstance(m, ModelResponse)]


async def run_models(
    records: list[AnnotationRecord],
    saver: Saver,
    concurrency: int,
    responses: dict[str, list[ModelResponse]],
) -> None:
    pending = [r for r in records if r.primary is None or r.review is None]
    if not pending:
        return
    # The instructions are identical for every item, so they form the cached prefix.
    annotator = Agent(
        PRIMARY_MODEL,
        instructions=ANNOTATOR_PROMPT,
        output_type=NativeOutput(AnnotatorResponse),
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
                try:
                    if record.primary is None:
                        primary_result = await annotator.run(render_item(record.item))
                        responses[PRIMARY_MODEL].extend(model_responses(primary_result))
                        record.primary = primary_result.output
                        record.primary_model = PRIMARY_MODEL
                        saver.save()
                    if record.review is None:
                        assert record.primary is not None
                        message = (
                            f"{render_item(record.item)}\n\n<annotation>\n"
                            f"{record.primary.model_dump_json(indent=2)}\n</annotation>"
                        )
                        review_result = await reviewer.run(message)
                        responses[REVIEW_MODEL].extend(model_responses(review_result))
                        record.review = review_result.output
                        record.review_model = REVIEW_MODEL
                        if not record.review.veto:
                            record.annotation = record.primary.annotation
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


def review_by_human(records: list[AnnotationRecord], saver: Saver) -> None:
    queue = [r for r in records if record_state(r) == "vetoed"]
    if queue:
        ReviewApp(queue, lambda: saver.save(force=True)).run()


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
    incomplete = sum(1 for r in records if r.review is None)
    approved = sum(1 for r in records if r.review is not None and not r.review.veto)
    vetoed = [r for r in records if r.review is not None and r.review.veto]
    resolved = sum(1 for r in vetoed if r.human is not None)
    table = Table(title="Annotation status")
    table.add_column("State")
    table.add_column("Items", justify="right")
    table.add_row("Approved by reviewer", str(approved))
    table.add_row("Vetoed, resolved by human", str(resolved))
    table.add_row("Vetoed, pending human", str(len(vetoed) - resolved))
    table.add_row("LLM calls incomplete", str(incomplete))
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
def run(
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
    saver = Saver(output_path, records)
    responses: dict[str, list[ModelResponse]] = {}
    try:
        asyncio.run(run_models(records, saver, concurrency, responses))
        saver.save(force=True)
        if not no_human:
            review_by_human(records, saver)
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
    """Browse the records in an annotated file."""
    with annotated_path.open() as fh:
        records = [
            AnnotationRecord.model_validate_json(line) for line in fh if line.strip()
        ]
    if state is not None:
        records = [r for r in records if record_state(r) == state]
    if not records:
        raise click.ClickException("No records to show.")
    ListApp(records).run()


if __name__ == "__main__":
    cli()
