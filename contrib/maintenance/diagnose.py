"""Print a runtime-diagnostics report for one dataset.

    python -m contrib.maintenance.diagnose <dataset_name>

Gives an agent (or human) the runtime state needed to start debugging a
dataset — run verdict, artifact links, issue digest, assertion drift — without
re-deriving zavod's archive semantics. The issues agent embeds the same report
into its prompts, so keep the output bounded and self-explanatory. This tool
surfaces state only; how to *fix* things is documented elsewhere.
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from .archive import (
    FAILED_RUN_ARTIFACTS,
    MAX_ISSUES,
    RUN_ARTIFACTS,
    VersionsInfo,
    artifact_url,
    fetch_artifact,
    get_issues,
    get_versions,
    head_artifact,
    version_timestamp,
)
from .datasets import get_code_path, get_path_from_name, read_dataset_meta, repo_path
from .issues import group_issues

MAX_ISSUE_GROUPS = 15
# How stale the newest archived run may be per crawl frequency before we
# suspect runs are dying without archiving anything (OOM, infra timeout).
STALE_AFTER = {
    "daily": timedelta(days=2),
    "weekly": timedelta(days=10),
    "monthly": timedelta(days=40),
}


def _fmt_ts(version_id: str) -> str:
    ts = version_timestamp(version_id)
    return ts.strftime("%Y-%m-%d %H:%M UTC") if ts is not None else "?"


def _fmt_size(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.1f}MB"
    if size >= 1024:
        return f"{size / 1024:.1f}kB"
    return f"{size}B"


def _verdict_section(
    name: str,
    meta: dict[str, Any],
    versions: VersionsInfo | None,
    issues: list[dict[str, Any]] | None,
) -> list[str]:
    if versions is None or versions.latest is None:
        return [
            f"# {name} — NOT IN THE PRODUCTION ARCHIVE",
            "",
            "No versions.json found — the dataset has never been deployed "
            "(or the name is misspelled). Remote sections are skipped.",
        ]

    lines: list[str] = []
    if versions.latest_failed:
        lines.append(f"# {name} — LATEST RUN FAILED")
        lines.append("")
        lines.append(f"Latest run:      {versions.latest} ({_fmt_ts(versions.latest)})")
        if versions.last_successful is None:
            lines.append(
                "Last successful: never — no run of this dataset has succeeded."
            )
        else:
            lines.append(
                f"Last successful: {versions.last_successful} "
                f"({_fmt_ts(versions.last_successful)})"
            )
            failed_runs = versions.runs_since_success()
            since = _fmt_ts(failed_runs[0]) if failed_runs else "?"
            count = f"{len(failed_runs)}"
            if versions.last_successful not in versions.items:
                count = f"more than {len(failed_runs)}"
            lines.append(f"Failing for {count} consecutive runs, since {since}.")
    else:
        levels: dict[str, int] = {}
        for issue in issues or []:
            level = str(issue.get("level", "unknown"))
            levels[level] = levels.get(level, 0) + 1
        summary = ", ".join(
            f"{n} {level}" + ("s" if n != 1 else "")
            for level, n in sorted(levels.items())
        )
        lines.append(f"# {name} — latest run succeeded ({summary or 'no issues'})")
        lines.append("")
        lines.append(f"Latest run: {versions.latest} ({_fmt_ts(versions.latest)})")

    if meta.get("disabled", False):
        lines.append("")
        lines.append(
            "**Dataset is `disabled: true`** — production runs no-op until re-enabled."
        )

    frequency = (meta.get("coverage") or {}).get("frequency")
    stale_after = STALE_AFTER.get(frequency or "")
    latest_ts = version_timestamp(versions.latest)
    if stale_after is not None and latest_ts is not None:
        age = datetime.now(timezone.utc) - latest_ts
        if age > stale_after:
            lines.append("")
            lines.append(
                f"**Stale:** newest archived run is {age.days} days old but the crawl "
                f"frequency is `{frequency}`. Runs may be dying before anything is "
                "archived (OOM, infra timeout) — the archive can look healthier than "
                "the dataset is."
            )
    return lines


def _files_section(
    name: str, yaml_path: str, meta: dict[str, Any], code_path: str | None
) -> list[str]:
    lines = ["## Dataset files", ""]
    lines.append(f"- Metadata YAML: `{yaml_path}`")
    if code_path is not None:
        lines.append(f"- Crawler code: `{code_path}`")
        try:
            if "zyte" in Path(code_path).read_text():
                lines.append(
                    "- Uses **Zyte** — production fetches route through the Zyte API; "
                    "local/CI fetch behaviour differs and CI has no API key."
                )
        except OSError:
            pass
    elif meta.get("entry_point") is not None:
        lines.append(
            f"- No dataset-local code: entry_point `{meta['entry_point']}` is an "
            "installed module (enrichment dataset). Only the YAML can be changed."
        )
    else:
        lines.append("- No crawler code (collection or externals-only dataset).")

    coverage = meta.get("coverage") or {}
    data = meta.get("data") or {}
    facts = [
        f"entry_point: `{meta.get('entry_point')}`",
        f"ci_test: `{meta.get('ci_test', True)}`",
        f"disabled: `{meta.get('disabled', False)}`",
        f"frequency: `{coverage.get('frequency')}`"
        + (
            f" (schedule `{coverage.get('schedule')}`)"
            if coverage.get("schedule")
            else ""
        ),
    ]
    lines.append("- " + " | ".join(facts))
    if data.get("url") is not None:
        lines.append(f"- Source data: {data['url']} ({data.get('format', '?')})")
    lines.append(f"- Reproduce locally: `zavod crawl {yaml_path}`")
    return lines


def _artifacts_section(name: str, versions: VersionsInfo) -> list[str]:
    lines: list[str] = []
    runs: list[tuple[str, str]] = []
    assert versions.latest is not None
    if versions.latest_failed:
        runs.append((versions.latest, "Latest (failed) run"))
        if versions.last_successful is not None:
            runs.append((versions.last_successful, "Last successful run"))
    else:
        runs.append((versions.latest, "Latest (successful) run"))

    for version_id, title in runs:
        failed = title.startswith("Latest (failed)")
        lines.append(f"## {title} {version_id} ({_fmt_ts(version_id)})")
        lines.append("")
        for resource in FAILED_RUN_ARTIFACTS if failed else RUN_ARTIFACTS:
            url = artifact_url(name, version_id, resource)
            try:
                status, size = head_artifact(name, version_id, resource)
            except Exception as exc:
                lines.append(f"- {resource} (check failed: {exc})")
                continue
            if status == 200:
                note = "200" if size is None else f"{_fmt_size(size)}, 200"
                lines.append(f"- {resource} ({note}) {url}")
            else:
                lines.append(f"- {resource} (absent, HTTP {status})")
        if failed:
            success_only = [r for r in RUN_ARTIFACTS if r not in FAILED_RUN_ARTIFACTS]
            lines.append(f"- {', '.join(success_only)}: never exported by failed runs")
        lines.append("")
    return lines[:-1]


def _clean_issue(issue: dict[str, Any]) -> dict[str, Any]:
    # `id` churns every run, `dataset`/`module` repeat the dataset name and
    # `timestamp` is summarized once for the whole run. Everything else —
    # notably the `data` payload with the actual dirty values — is kept,
    # except the dataset name repeated inside `data` too.
    drop = ("id", "dataset", "module", "timestamp")
    cleaned = {k: v for k, v in issue.items() if k not in drop}
    data = cleaned.get("data")
    if isinstance(data, dict):
        data = {k: v for k, v in data.items() if k != "dataset"}
        if data:
            cleaned["data"] = data
        else:
            del cleaned["data"]
    return cleaned


def _issues_section(
    name: str, issues: list[dict[str, Any]] | None, max_issues: int
) -> list[str]:
    if issues is None:
        return ["## Issues", "", "No issues.log found for the latest run."]
    truncated = len(issues) > MAX_ISSUES
    if truncated:
        issues = issues[:MAX_ISSUES]
        total = f"more than {MAX_ISSUES} — reading stopped there"
    else:
        total = f"{len(issues)} total"
    lines = [f"## Issues of the latest run ({total})", ""]
    if len(issues) == 0:
        return lines + ["The latest run logged no issues."]

    timestamps = sorted({str(i.get("timestamp", ""))[:16] for i in issues})
    if timestamps:
        if timestamps[0] == timestamps[-1]:
            lines.append(f"All logged at {timestamps[0]}.")
        else:
            lines.append(f"Logged between {timestamps[0]} and {timestamps[-1]}.")
        lines.append("")

    if len(issues) <= max_issues:
        cleaned = [_clean_issue(issue) for issue in issues]
        lines.append("```yaml")
        lines.append(
            yaml.safe_dump(
                cleaned, sort_keys=False, allow_unicode=True, width=100
            ).rstrip()
        )
        lines.append("```")
    else:
        groups = group_issues(issues)
        lines.append(
            f"Too many to inline — {len(groups)} distinct message patterns, "
            "one example each:"
        )
        lines.append("")
        for pattern, members in groups[:MAX_ISSUE_GROUPS]:
            example = members[0]
            line = f"- **{len(members)}×** [{example.get('level')}] {example.get('message')}"
            data = example.get("data")
            if data:
                # One example's payload, truncated — the dirty values live here;
                # the full set is in the linked issues.json.
                payload = json.dumps(data, ensure_ascii=False)
                if len(payload) > 200:
                    payload = payload[:200] + "…"
                line += f" — e.g. `{payload}`"
            lines.append(line)
        if len(groups) > MAX_ISSUE_GROUPS:
            rest = sum(len(m) for _, m in groups[MAX_ISSUE_GROUPS:])
            lines.append(
                f"- … {len(groups) - MAX_ISSUE_GROUPS} more patterns ({rest} issues)"
            )
        if truncated:
            lines.append("")
            lines.append(
                f"Counts cover only the first {MAX_ISSUES} issues; the full set "
                "is in the linked issues.log / issues.json."
            )
    return lines


def _assertion_values(
    metric: str, config: Any, stats: dict[str, Any]
) -> list[tuple[str, float, float]]:
    """Yield (key, threshold, last-good value) per configured assertion entry.

    Mirrors zavod/validators/assertions.py so the report compares the exact
    quantities the validator checks.
    """
    things = stats["things"]
    if metric == "entity_count":
        return [("", float(config), float(things["total"]))]
    if metric == "countries":
        return [("", float(config), float(len(things["countries"])))]
    if metric == "schema_entities":
        counts = {i["name"]: i["count"] for i in things["schemata"]}
        return [(k, float(t), float(counts.get(k, 0))) for k, t in config.items()]
    if metric == "country_entities":
        counts = {i["code"]: i["count"] for i in things["countries"]}
        return [(k, float(t), float(counts.get(k, 0))) for k, t in config.items()]
    if metric in ("entities_with_prop", "property_fill_rate"):
        field = "count" if metric == "entities_with_prop" else "fill_rate"
        values = {
            (i["schema"], i["property"]): i[field] for i in things["entities_with_prop"]
        }
        return [
            (f"{schema}.{prop}", float(t), float(values.get((schema, prop), 0)))
            for schema, props in config.items()
            for prop, t in props.items()
        ]
    raise ValueError(f"Unknown assertion metric: {metric}")


def _assertions_section(
    name: str, meta: dict[str, Any], versions: VersionsInfo
) -> list[str]:
    assertions = meta.get("assertions")
    if not assertions:
        return ["## Assertions", "", "The dataset declares no assertions."]
    if versions.last_successful is None:
        return [
            "## Assertions",
            "",
            "No successful run — no statistics to compare the assertions against.",
        ]
    stats = fetch_artifact(name, versions.last_successful, "statistics.json")
    if stats is None:
        return [
            "## Assertions",
            "",
            "Could not fetch statistics.json of the last successful run.",
        ]

    lines = [
        f"## Assertions vs statistics of the last successful run "
        f"({versions.last_successful})",
        "",
        "| metric | key | bound | threshold | last-good value | status |",
        "|---|---|---|---|---|---|",
    ]
    for bound, metrics in assertions.items():
        for metric, config in metrics.items():
            for key, threshold, value in _assertion_values(metric, config, stats):
                if bound == "min":
                    violated, close = value < threshold, value < threshold * 1.1
                elif bound == "max":
                    violated, close = value > threshold, value > threshold * 0.9
                else:
                    raise ValueError(f"Unknown assertion bound: {bound}")
                status = (
                    "VIOLATED" if violated else ("close to bound" if close else "ok")
                )
                lines.append(
                    f"| {metric} | {key} | {bound} | {threshold:g} "
                    f"| {value:g} | {status} |"
                )
    lines.append("")
    lines.append(
        "Values are from the last *successful* run — a failing assertion in the "
        "latest run may have drifted further."
    )
    return lines


def _history_section(yaml_path: str) -> list[str]:
    dataset_dir = Path(yaml_path).parent.as_posix()
    try:
        shallow = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=repo_path,
        )
        if shallow.stdout.strip() == "true":
            return [
                f"## Recent commits touching {dataset_dir}",
                "",
                "(shallow clone — history unavailable)",
            ]
        log = subprocess.run(
            [
                "git",
                "log",
                "-5",
                "--oneline",
                "--date=short",
                "--format=%h %ad %s",
                "--",
                dataset_dir,
            ],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=repo_path,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if log.returncode != 0 or log.stdout.strip() == "":
        return []
    lines = [f"## Recent commits touching {dataset_dir}", ""]
    lines += [f"- {line}" for line in log.stdout.strip().splitlines()]
    return lines


def build_report(name: str, max_issues: int = 10) -> str:
    """Render the full diagnostics report for one dataset as markdown.

    Pure in the sense that matters for embedding into agent prompts: repo and
    network reads only, bounded output, and remote failures degrade to a note
    in the affected section instead of raising.
    """
    yaml_path = get_path_from_name(name)
    meta = read_dataset_meta(yaml_path)
    code_path = get_code_path(yaml_path, meta.get("entry_point"))

    try:
        versions = get_versions(name)
    except Exception as exc:
        versions = None
        archive_error: str | None = f"Could not read versions.json: {exc}"
    else:
        archive_error = None

    issues: list[dict[str, Any]] | None = None
    if versions is not None and versions.latest is not None:
        try:
            issues = get_issues(name, versions.latest)
        except Exception:
            issues = None

    sections = [_verdict_section(name, meta, versions, issues)]
    if archive_error is not None:
        sections.append([archive_error])
    sections.append(_files_section(name, yaml_path, meta, code_path))
    if versions is not None and versions.latest is not None:
        try:
            sections.append(_artifacts_section(name, versions))
        except Exception as exc:
            sections.append(["## Artifacts", "", f"(section unavailable: {exc})"])
        sections.append(_issues_section(name, issues, max_issues))
        try:
            sections.append(_assertions_section(name, meta, versions))
        except Exception as exc:
            sections.append(["## Assertions", "", f"(section unavailable: {exc})"])
    history = _history_section(yaml_path)
    if history:
        sections.append(history)

    return "\n\n".join("\n".join(section) for section in sections) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a runtime-diagnostics report for one dataset."
    )
    parser.add_argument("name", help="dataset name, e.g. om_parliament")
    parser.add_argument(
        "--max-issues",
        type=int,
        default=10,
        help="inline the full issues when there are at most this many (default 10)",
    )
    args = parser.parse_args()
    try:
        report = build_report(args.name, max_issues=args.max_issues)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    print(report, end="")


if __name__ == "__main__":
    main()
