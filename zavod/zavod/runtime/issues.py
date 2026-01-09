import orjson
from pathlib import Path
from rigour.time import utc_now, datetime_iso
from banal import is_mapping, hash_data
from datetime import datetime
from typing import Any, Dict, Generator, Optional, TypedDict, BinaryIO, cast

from zavod.meta import Dataset
from zavod.archive import dataset_resource_path, get_dataset_artifact
from zavod.archive import ISSUES_LOG, ISSUES_FILE


class Issue(TypedDict):
    id: int
    timestamp: datetime
    level: str
    module: Optional[str]
    dataset: str
    message: Optional[str]
    entity_id: Optional[str]
    entity_schema: Optional[str]
    data: Dict[str, Any]


class DatasetIssues(object):
    """A log of issues that occurred during the running and export of a dataset."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.fh: Optional[BinaryIO] = None
        get_dataset_artifact(self.dataset.name, ISSUES_LOG)

    def write(self, event: Dict[str, Any]) -> None:
        if self.fh is None:
            path = dataset_resource_path(self.dataset.name, ISSUES_LOG)
            self.fh = open(path, "ab")

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
        log_path = dataset_resource_path(self.dataset.name, ISSUES_LOG)
        with open(log_path, "w") as fh:
            fh.flush()
        file_path = dataset_resource_path(self.dataset.name, ISSUES_FILE)
        file_path.unlink(missing_ok=True)

    def close(self) -> None:
        """Close the issues log file."""
        if self.fh is not None:
            self.fh.close()
        self.fh = None

    def all(self) -> Generator[Issue, None, None]:
        """Iterate over all issues in the log."""
        self.close()
        path = get_dataset_artifact(self.dataset.name, ISSUES_LOG)
        if not path.is_file():
            return
        with open(path, "rb") as fh:
            for line in fh:
                yield cast(Issue, orjson.loads(line))

    def by_level(self) -> Dict[str, int]:
        """Count the number of issues by severity level."""
        levels: Dict[str, int] = {}
        for issue in self.all():
            level = issue.get("level")
            if level is not None:
                levels[level] = levels.get(level, 0) + 1
        return levels

    def export(self, path: Optional[Path] = None) -> None:
        """Export the issues log to a consolidated file."""
        if path is None:
            path = dataset_resource_path(self.dataset.name, ISSUES_FILE)
        with open(path, "wb") as fh:
            issues = list(self.all())
            fh.write(orjson.dumps({"issues": issues}))
