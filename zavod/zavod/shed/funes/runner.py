"""The funes inspection runner: one zavod crawl over due candidates.

The runner is a sync crawler entry point around one ``asyncio.run``:
the capture and model coroutines need an event loop, and Pravda's
browser connections are bound to the loop that created them, so the
loop spans the whole candidate loop. All state-store writes — snapshot
and attempt rows — go through the sync ``context.db`` session from
inside the coroutine, which merely blocks the loop, a tolerable cost
for a sequential runner; each candidate ends with ``context.flush()``
so no capture or model run sits inside an idle write transaction, and
each candidate's snapshot and attempt rows commit together.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from typing import cast

from pydantic_ai import ModelSettings
from pydantic_ai.settings import ThinkingEffort
from pravda import Pravda, Snapshot, Storage
from sqlalchemy import insert

from zavod import settings
from zavod.context import Context
from zavod.exc import ConfigurationException
from zavod.shed.funes.agents import Brief
from zavod.shed.funes.attempts import DueCandidate, select_due_candidates
from zavod.shed.funes.capture import first_error_line, inspectability_issue
from zavod.shed.funes.catalogue import sync_catalogue
from zavod.shed.funes.definitions import DatasetDefinition, dataset_definition
from zavod.shed.funes.emit import emit_hit
from zavod.shed.funes.extract import (
    BrokenSnapshot,
    ExtractionDependencies,
    Hit,
    extraction_agent,
)
from zavod.shed.funes.model import (
    InspectionOutcome,
    SnapshotStatus,
    attempt_table,
    snapshot_table,
)
from zavod.shed.funes.outline import build_outline, har_resource_media_types
from zavod.shed.funes.page import build_prompt, metadata_from_html

Capture = Callable[[str], Awaitable[Snapshot]]
"""One fresh Pravda snapshot per candidate URL."""

ReadResource = Callable[[str], Awaitable[bytes]]
"""Artifact read access for one capture's storage backend."""


@dataclass(frozen=True)
class FunesRuntime:
    """Process-level runner configuration."""

    browser_ws_url: str
    storage_base_path: str
    model: str
    thinking: ThinkingEffort


def load_runtime() -> FunesRuntime:
    browser_ws_url = settings.PRAVDA_BROWSER_WS_URL
    if browser_ws_url is None:
        raise ConfigurationException("No $PRAVDA_BROWSER_WS_URL provided.")
    storage_base_path = settings.PRAVDA_STORAGE_BASE_PATH
    if storage_base_path is None:
        raise ConfigurationException("No $PRAVDA_STORAGE_BASE_PATH provided.")
    model = settings.FUNES_MODEL
    if model is None:
        raise ConfigurationException("No $FUNES_MODEL provided.")
    thinking = settings.FUNES_THINKING
    if thinking is None:
        raise ConfigurationException("No $FUNES_THINKING provided.")
    return FunesRuntime(
        browser_ws_url=browser_ws_url,
        storage_base_path=storage_base_path,
        model=model,
        thinking=cast(ThinkingEffort, thinking),
    )


def crawl(context: Context) -> None:
    """Crawl one funes dataset: capture and judge every due candidate."""
    definition = dataset_definition(context.dataset)
    if definition is None:
        raise ValueError(
            f"Dataset {context.dataset.name} has no config.funes brief; "
            "cannot run the funes inspection runner"
        )
    sync_catalogue(context, definition)
    candidates = select_due_candidates(context)
    runtime = load_runtime()
    asyncio.run(_inspect_candidates(context, definition, candidates, runtime))


async def _inspect_candidates(
    context: Context,
    definition: DatasetDefinition,
    candidates: list[DueCandidate],
    runtime: FunesRuntime,
) -> None:
    """Capture and judge each due candidate inside one shared event loop."""
    storage = Storage.from_url(runtime.storage_base_path)
    pravda = Pravda(runtime.browser_ws_url, storage)
    capture: Capture = pravda.capture
    read_resource: ReadResource = storage.read_artifact
    for candidate in candidates:
        await _inspect_candidate(
            context,
            capture=capture,
            read_resource=read_resource,
            model=runtime.model,
            thinking=runtime.thinking,
            brief=Brief(
                people_sought=definition.people_sought,
                subject_label=definition.subject_label,
                subject=candidate.subject_name,
            ),
            candidate_id=candidate.candidate_id,
            url=candidate.url,
        )
        context.flush()


async def _inspect_candidate(
    context: Context,
    *,
    capture: Capture,
    read_resource: ReadResource,
    model: str,
    thinking: ThinkingEffort,
    brief: Brief,
    candidate_id: uuid.UUID,
    url: str,
) -> None:
    """Capture one candidate URL and judge it against its inspection brief."""
    attempt_id = uuid.uuid4()
    context.log.info("Capturing", url=url)
    snapshot = await capture(url)
    # Failed captures are evidence too: they arrive with ``error`` set
    # and no artifacts, and persist like any other snapshot.
    context.db.execute(insert(snapshot_table).values(**asdict(snapshot)))

    issue = inspectability_issue(snapshot)
    if issue is not None:
        context.db.execute(
            insert(attempt_table).values(
                id=attempt_id,
                candidate_id=candidate_id,
                snapshot_id=snapshot.id,
                status=SnapshotStatus.BROKEN,
                reason=issue,
            )
        )
        context.log.info("Broken snapshot", url=url, reason=issue)
        return

    if snapshot.final_url is None or snapshot.rendered_html is None:
        raise ValueError(
            f"inspectable snapshot {snapshot.id} of {url!r} has no final URL "
            "or rendered HTML artifact"
        )
    html = (await read_resource(snapshot.rendered_html)).decode("utf-8")
    metadata = metadata_from_html(
        url,
        html,
        final_url=snapshot.final_url,
        http_status=snapshot.http_status,
        capture_error=first_error_line(snapshot.error),
    )
    outline = build_outline(snapshot.final_url, html, snapshot.http_archive)
    deps = ExtractionDependencies(
        brief=brief,
        read_resource=read_resource,
        resource_media_types=har_resource_media_types(snapshot.http_archive),
    )
    context.log.info("Extracting", url=snapshot.final_url)
    run = await extraction_agent.run(
        build_prompt(metadata, outline),
        model=model,
        model_settings=ModelSettings(thinking=thinking),
        run_id=str(attempt_id),
        deps=deps,
    )

    if isinstance(run.output, BrokenSnapshot):
        context.db.execute(
            insert(attempt_table).values(
                id=attempt_id,
                candidate_id=candidate_id,
                snapshot_id=snapshot.id,
                status=SnapshotStatus.BROKEN,
                reason=run.output.reason,
                model=model,
            )
        )
        context.log.info(
            "Broken snapshot", url=snapshot.final_url, reason=run.output.reason
        )
        return

    if isinstance(run.output, Hit):
        emit_hit(context, run.output, source_url=url)
        context.db.execute(
            insert(attempt_table).values(
                id=attempt_id,
                candidate_id=candidate_id,
                snapshot_id=snapshot.id,
                status=SnapshotStatus.USABLE,
                outcome=InspectionOutcome.HIT,
                model=model,
            )
        )
        context.log.info(
            "Hit",
            url=snapshot.final_url,
            persons=len(run.output.persons),
        )
        return

    context.db.execute(
        insert(attempt_table).values(
            id=attempt_id,
            candidate_id=candidate_id,
            snapshot_id=snapshot.id,
            status=SnapshotStatus.USABLE,
            outcome=InspectionOutcome.MISS,
            reason=run.output.reason,
            model=model,
        )
    )
    context.log.info("Miss", url=snapshot.final_url, reason=run.output.reason)
