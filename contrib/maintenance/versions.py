"""Print a table of a dataset's recent archived runs, newest first.

    python -m contrib.maintenance.versions <dataset_name> [-n 20]

The diagnose report covers the latest run and the last successful one; this is
the history companion: it walks back over past runs so you can see how the
dataset behaved over time — when entity counts moved, how often runs fail, when
a schema first appeared. Each row links its `index.json`, the entry point for
digging into a single run.

The archived version history comes in bounded windows, so `--start` resumes the
walk at a version a previous invocation printed.
"""

import argparse
import sys
from typing import Any

from .archive import artifact_url, fetch_artifact, iter_versions

DEFAULT_COUNT = 20


def _fmt_export(index: dict[str, Any]) -> str:
    """The run's export timestamp, minute precision."""
    stamp = index.get("last_export")
    if not isinstance(stamp, str):
        return "?"
    return stamp.replace("T", " ")[:16]


def _fmt_count(value: Any) -> str:
    return str(value) if isinstance(value, int) else "-"


def _schema_counts(name: str, version_id: str) -> dict[str, int]:
    """Thing counts by schema from a run's statistics.json.

    Empty for failed runs — they never export statistics. Only *thing* schemata
    are counted there, so intervals like Sanction have no count.
    """
    stats = fetch_artifact(name, version_id, "statistics.json")
    if stats is None:
        return {}
    return {item["name"]: item["count"] for item in stats["things"]["schemata"]}


def build_table(
    name: str,
    count: int = DEFAULT_COUNT,
    start: str | None = None,
    schemata: list[str] | None = None,
) -> str:
    """Render the run history of one dataset as a markdown table."""
    schemata = schemata or []
    versions: list[str] = []
    for version_id in iter_versions(name, start=start):
        versions.append(version_id)
        if len(versions) >= count:
            break
    if not versions:
        where = f" at or before {start}" if start is not None else ""
        raise RuntimeError(f"No archived runs found for {name}{where}.")

    columns = ["version", "exported_at", "result", "entities", "targets"]
    columns += schemata + ["index.json"]
    lines = [
        f"# {len(versions)} archived runs of {name}, newest first",
        "",
        "| " + " | ".join(columns) + " |",
        "|" + "---|" * len(columns),
    ]
    for version_id in versions:
        try:
            index = fetch_artifact(name, version_id, "index.json")
        except Exception as exc:
            index = None
            note = f"unreadable: {exc}"
        else:
            note = "absent"
        if index is None:
            # A version listed in a window whose index is gone or unreadable —
            # old runs of removed datasets do that. The row still records that
            # the run happened.
            cells = [version_id] + ["-"] * (len(columns) - 2) + [note]
            lines.append("| " + " | ".join(cells) + " |")
            continue
        cells = [
            version_id,
            _fmt_export(index),
            str(index.get("result", "?")),
            _fmt_count(index.get("entity_count")),
            _fmt_count(index.get("target_count")),
        ]
        if schemata:
            try:
                counts = _schema_counts(name, version_id)
            except Exception:
                # One unreadable statistics.json blanks its schema cells rather
                # than aborting the whole table.
                counts = {}
            cells += [_fmt_count(counts.get(schema)) for schema in schemata]
        cells.append(artifact_url(name, version_id, "index.json"))
        lines.append("| " + " | ".join(cells) + " |")

    lines.append("")
    lines.append(
        f"To walk further back: `--start {versions[-1]}` (or an older version "
        "ID, if you know one)."
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a table of a dataset's recent archived runs."
    )
    parser.add_argument("name", help="dataset name, e.g. us_ofac_sdn")
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        help=f"how many runs to walk back over (default {DEFAULT_COUNT})",
    )
    parser.add_argument(
        "--start",
        metavar="VERSION",
        help="begin the walk at this archived version instead of the newest run",
    )
    parser.add_argument(
        "--schema",
        action="append",
        metavar="SCHEMA",
        help="add a column with this schema's entity count, read from each run's "
        "statistics.json (repeatable, e.g. --schema Person --schema Company)",
    )
    args = parser.parse_args()
    try:
        table = build_table(
            args.name, count=args.count, start=args.start, schemata=args.schema
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    print(table, end="")


if __name__ == "__main__":
    main()
