"""Generate the GitHub Actions matrix of per-dataset issue-fixing agent tasks.

Invoked by .github/workflows/issues-agent.yml as:

    python -m contrib.maintenance.issues_agent

Emits the matrix JSON on stdout and writes each task's prompt to
<prompts-dir>/<dataset>.md; everything human-readable goes to stderr. The
prompts travel to the run-tasks job as a workflow artifact rather than inside
the matrix: they embed diagnostic reports full of arbitrary source data, and
GitHub silently drops a job output when it spots anything resembling a secret
in it ("Skip output 'matrix' since it may contain secret"), which breaks the
fromJson() strategy expression downstream.

This module holds only agent policy — model/turn selection, branch naming and
PR dedup. The shared mechanics live in the sibling modules of this package.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from jinja2 import Template

from .archive import MAX_ISSUES, get_issues, get_versions, iter_catalog_datasets
from .datasets import get_code_path, get_path_from_name, read_dataset_meta
from .diagnose import build_report
from .github import (
    branch_prefix,
    dataset_has_open_pr,
    get_open_autofix_branches,
    get_outage_datasets,
    has_closed_pr_for_branch,
)
from .issues import is_issue_ignored, issues_checksum

# Match compute to task difficulty. Lookup/assertion edits on YAML are mechanical,
# so a mid-tier model with few turns suffices. Datasets with a crawler may need an
# actual code fix — stronger reasoning, plus turns to run mypy, crawl, and iterate.
MODEL_LOOKUP = "claude-sonnet-4-6"
MODEL_CODE = "claude-opus-4-8"
MAX_TURNS_LOOKUP = 30
MAX_TURNS_CODE_VERIFIABLE = 100  # ci_test true: edit, then crawl-verify and iterate
MAX_TURNS_CODE_BLIND = 60  # ci_test false: edit + mypy/ruff, no crawl loop
# Rendered per dataset with the diagnostic context (paths, branch, ci_test) so
# the prompt can branch between lookup-only and code-fix instructions. autoescape
# stays off (default) so markdown and YAML examples pass through untouched.
PROMPT = Template(
    (Path(__file__).parent / "prompt.md").read_text(),
    trim_blocks=True,
    lstrip_blocks=True,
)


def log(message: str) -> None:
    """Emit a diagnostic line to stderr.

    stdout carries the matrix JSON that GitHub Actions captures, so anything we
    want a human to read in the run log has to go to stderr to avoid corrupting it.
    """
    print(message, file=sys.stderr)


def index_jobs(prompts_dir: Path) -> None:
    outage_datasets = get_outage_datasets()
    open_autofix_branches = get_open_autofix_branches()
    tasks: list[Any] = []

    for dataset in iter_catalog_datasets():
        name = dataset.get("name")
        if not name:
            continue
        # The catalogs are only a discovery index: a cheap pre-filter for
        # which datasets are worth a look. The authoritative issue contents
        # and counts come from the latest run in versions.json below — the
        # same run the embedded diagnostic report describes, which can be
        # fresher than the catalog snapshot.
        levels = dataset.get("issue_levels", {})
        if levels.get("warning", 0) + levels.get("error", 0) == 0:
            continue
        if name in outage_datasets:
            log(f"Documented outage: {name}")
            continue

        if dataset_has_open_pr(name, open_autofix_branches):
            log(f"Open PR exists: {name}")
            continue

        versions = get_versions(name)
        if versions is None or versions.latest is None:
            log(f"No archived runs: {name}")
            continue
        issues = get_issues(name, versions.latest) or []
        if len(issues) > MAX_ISSUES:
            log(f"Fubar: {name} ({MAX_ISSUES}+ issues in latest run)")
            continue
        errors = sum(1 for i in issues if i.get("level") == "error")
        warnings = sum(1 for i in issues if i.get("level") == "warning")
        if warnings + errors == 0:
            log(f"Latest run is clean: {name}")
            continue
        # Don't spawn an agent when a run produced nothing anyone can act on.
        # Mixed runs (an ignored infrastructure error alongside real issues)
        # still get a task.
        if all(is_issue_ignored(i) for i in issues):
            log(f"Only transient infrastructure issues: {name}")
            continue

        checksum = issues_checksum(issues)
        branch = f"{branch_prefix(name)}{checksum[:10]}"
        if has_closed_pr_for_branch(branch):
            log(f"Already handled (closed PR): {name} ({branch})")
            continue

        path = get_path_from_name(name)
        crawler_dir = Path(path).parent.as_posix()
        dataset_meta = read_dataset_meta(path)
        entry_point = dataset_meta.get("entry_point")
        ci_test = dataset_meta.get("ci_test", True)
        code_path = get_code_path(path, entry_point)

        # Only crawler datasets can require code, so only they need zavod
        # installed (for mypy and crawling) and the heavier model/turn budget.
        if code_path is not None:
            model = MODEL_CODE
            max_turns = MAX_TURNS_CODE_VERIFIABLE if ci_test else MAX_TURNS_CODE_BLIND
            needs_zavod = True
        else:
            model = MODEL_LOOKUP
            max_turns = MAX_TURNS_LOOKUP
            needs_zavod = False

        deploy_config = dataset_meta.get("deploy", {})
        max_turns = deploy_config.get("issues_turns", max_turns)
        if max_turns > MAX_TURNS_CODE_VERIFIABLE:
            model = MODEL_CODE

        # The report is the prompt's single source of runtime facts (run
        # verdict, artifact links, the issues themselves); the template only
        # carries instructions and the values its conditionals/scope rules
        # need. It describes the same versions.json-latest run the checksum
        # above was computed from.
        prompt = PROMPT.render(
            name=name,
            report=build_report(name).rstrip(),
            yaml_path=path,
            crawler_dir=crawler_dir,
            branch=branch,
            code_path=code_path,
            ci_test=ci_test,
        )
        counts = [
            f"{n} {label}" + ("s" if n != 1 else "")
            for n, label in ((errors, "error"), (warnings, "warning"))
            if n > 0
        ]
        title = f"[{name}]: {', '.join(counts)}"
        (prompts_dir / f"{name}.md").write_text(prompt)
        tasks.append(
            {
                "dataset": name,
                "name": title,
                "branch": branch,
                "model": model,
                "max_turns": max_turns,
                "needs_zavod": needs_zavod,
            }
        )

    print(json.dumps(tasks))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate the issues-agent task matrix and prompt files."
    )
    parser.add_argument(
        "--prompts-dir",
        type=Path,
        default=Path("task-prompts"),
        help="directory to write one <dataset>.md prompt file per task into",
    )
    args = parser.parse_args()
    args.prompts_dir.mkdir(parents=True, exist_ok=True)
    index_jobs(args.prompts_dir)
