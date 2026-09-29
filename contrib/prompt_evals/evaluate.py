"""Run a crawler's current prompt against its fixtures and report agreement with
the human-accepted extractions."""

import json
import threading
from pathlib import Path
from typing import Any

from pydantic_evals.reporting import EvaluationReport
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt
from zavod.meta import Dataset

from contrib.prompt_evals.crawler import CrawlerPrompt
from contrib.prompt_evals.fixtures import load_fixtures
from contrib.prompt_evals.models import CaseInputs, CaseMeta, Extraction, FixtureDataset


def select_cases(
    dataset: FixtureDataset,
    names: list[str],
    edited_only: bool,
    limit: int | None,
) -> FixtureDataset:
    cases = dataset.cases
    if names:
        cases = [c for c in cases if c.name in names]
    if edited_only:
        cases = [c for c in cases if c.metadata is not None and c.metadata.edited]
    if limit is not None:
        cases = cases[:limit]
    return FixtureDataset(name=dataset.name, cases=cases, evaluators=dataset.evaluators)


class ContextPool:
    """One zavod Context per worker thread.

    pydantic-evals runs the task in a thread pool, and a Context owns a single
    database connection that must not be shared between threads. Each thread
    gets its own Context and checkpoints after every case, so cached model
    responses survive even if the run is interrupted.
    """

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self._local = threading.local()
        self._lock = threading.Lock()
        self.contexts: list[Context] = []

    def get(self) -> Context:
        context: Context | None = getattr(self._local, "context", None)
        if context is None:
            context = Context(self.dataset)
            self._local.context = context
            with self._lock:
                self.contexts.append(context)
        return context

    def close(self) -> None:
        for context in self.contexts:
            context.close()


def run(
    dataset: Dataset,
    crawler: CrawlerPrompt,
    fixtures_file: Path,
    names: list[str],
    edited_only: bool,
    limit: int | None,
    model: str,
    max_concurrency: int,
    fresh: bool = False,
    outputs: dict[str, Extraction] | None = None,
) -> EvaluationReport[CaseInputs, Extraction, CaseMeta]:
    """Run the prompt over the fixtures. When `outputs` is given, the model output
    for each case is stored in it keyed by case name."""
    fixtures = select_cases(load_fixtures(fixtures_file), names, edited_only, limit)
    base_dir = fixtures_file.parent
    pool = ContextPool(dataset)
    name_by_source = {c.inputs.source_file: str(c.name) for c in fixtures.cases}

    def task(inputs: CaseInputs) -> Extraction:
        context = pool.get()
        source = (base_dir / inputs.source_file).read_text()
        result = run_typed_text_prompt(
            context,
            crawler.prompt,
            source,
            crawler.response_type,
            crawler.max_tokens,
            # cache_days=0 disables cache reads while still storing the response.
            cache_days=0 if fresh else 100,
            model=model,
        )
        context.flush()
        dumped = result.model_dump()
        if outputs is not None:
            outputs[name_by_source[inputs.source_file]] = dumped
        return dumped

    try:
        return fixtures.evaluate_sync(task, max_concurrency=max_concurrency, name=model)
    finally:
        pool.close()


def summarise(
    report: EvaluationReport[CaseInputs, Extraction, CaseMeta],
) -> dict[str, Any]:
    """A compact, JSON-serialisable view of a report for saving as a baseline."""
    cases: dict[str, Any] = {}
    for case in report.cases:
        cases[case.name] = {
            "scores": {k: v.value for k, v in case.scores.items()},
            "assertions": {k: v.value for k, v in case.assertions.items()},
            "reasons": {
                k: v.reason
                for k, v in {**case.scores, **case.assertions}.items()
                if v.reason
            },
        }
    for failure in report.failures:
        cases[failure.name] = {"error": str(failure.error_message)}
    return {"name": report.name, "cases": cases}


def save_summary(
    report: EvaluationReport[CaseInputs, Extraction, CaseMeta], path: Path
) -> None:
    path.write_text(json.dumps(summarise(report), indent=2, sort_keys=True))


def compare(current: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    """Lines describing cases whose scores moved between baseline and current."""
    lines: list[str] = []
    for name, cur in sorted(current["cases"].items()):
        base = baseline["cases"].get(name)
        if base is None:
            lines.append(f"{name}: new case")
            continue
        if "error" in cur or "error" in base:
            if cur.get("error") != base.get("error"):
                lines.append(
                    f"{name}: error changed: {base.get('error')} -> {cur.get('error')}"
                )
            continue
        for metric, value in {**cur["scores"], **cur["assertions"]}.items():
            old = {**base["scores"], **base["assertions"]}.get(metric)
            if old is not None and old != value:
                arrow = "improved" if value > old else "REGRESSED"
                reason = cur["reasons"].get(metric, "")
                lines.append(
                    f"{name}: {metric} {old:.2f} -> {value:.2f} {arrow} {reason}".rstrip()
                )
    return lines
