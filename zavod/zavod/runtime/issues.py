import orjson
from pathlib import Path
from rigour.time import utc_now, datetime_iso
from banal import is_mapping, hash_data
from datetime import datetime
from typing import Any, TypedDict, BinaryIO, cast
from collections.abc import Generator

from zavod.meta import Dataset
from zavod.archive import (
    dataset_artifact_path,
)
from zavod.archive import ISSUES_LOG, ISSUES_FILE


class Issue(TypedDict):
    id: int
    timestamp: datetime
    level: str
    module: str | None
    dataset: str
    message: str | None
    entity_id: str | None
    entity_schema: str | None
    data: dict[str, Any]


class DatasetIssues:
    """A log of issues that occurred during the running and export of a dataset."""

    def __init__(self, dataset: Dataset, version: str) -> None:
        self.dataset = dataset
        self.version = version
        self.log_path = dataset_artifact_path(
            self.dataset.name, self.version, ISSUES_LOG
        )
        self.file_path = dataset_artifact_path(
            self.dataset.name, self.version, ISSUES_FILE
        )
        self.fh: BinaryIO | None = None

    def write(self, event: dict[str, Any]) -> None:
        if self.fh is None:
            self.fh = open(self.log_path, "ab")

        data = dict(event)  # copy so we can pop without side effects
        data.pop("_record", None)
        report_issue = data.pop("report_issue", True)
        if not report_issue:
            return
        record = {
            "timestamp": datetime_iso(utc_now()),
            "module": data.pop("logger", None),
            "level": data.pop("level"),
            "message": data.pop("event", None),
            "dataset": self.dataset.name,
        }
        entity = data.pop("entity", None)
        if is_mapping(entity):
            record["entity"] = entity
        elif isinstance(entity, str):
            record["entity"] = {"id": entity}
        record["data"] = data
        record["id"] = hash_data(record)
        # No `default` so we crash if something wasn't made JSON-serializable
        # (and thus redacted) just as another layer of protection.
        # But serializability and redaction _should_ be guaranteed here.
        out = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        self.fh.write(out)

    def clear(self) -> None:
        """Clear (delete) the issues log file."""
        self.close()
        with open(self.log_path, "w") as fh:
            fh.flush()
        self.file_path.unlink(missing_ok=True)

    def close(self) -> None:
        """Close the issues log file."""
        if self.fh is not None:
            self.fh.close()
        self.fh = None

    def all(self) -> Generator[Issue, None, None]:
        """Iterate over all issues in the log."""
        self.close()
        if not self.log_path.is_file():
            return
        with open(self.log_path, "rb") as fh:
            for line in fh:
                yield cast(Issue, orjson.loads(line))

    def by_level(self) -> dict[str, int]:
        """Count the number of issues by severity level."""
        levels: dict[str, int] = {}
        for issue in self.all():
            level = issue.get("level")
            if level is not None:
                levels[level] = levels.get(level, 0) + 1
        return levels

    def export(self, path: Path | None = None) -> None:
        """Export the issues log to a consolidated file."""
        path = path or self.file_path
        with open(path, "wb") as fh:
            issues = list(self.all())
            fh.write(orjson.dumps({"issues": issues}))
