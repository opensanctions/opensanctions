import orjson
from pathlib import Path
from banal import is_mapping, hash_data
from datetime import datetime
from typing import Any, Dict, Generator, Optional, TypedDict, BinaryIO, cast
from nomenklatura.util import datetime_iso

from zavod.meta import Dataset
from zavod.archive import dataset_resource_path, get_dataset_resource
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
        self.path = dataset_resource_path(dataset.name, ISSUES_LOG)
        self.fh: Optional[BinaryIO] = None

    def write(self, event: Dict[str, Any]) -> None:
        if self.fh is None:
            self.fh = open(self.path, "ab")
        data = dict(event)
        for key, value in data.items():
            if key == "dataset" and value == self.dataset.name:
                continue
            if hasattr(value, "to_dict"):
                value = value.to_dict()
            if isinstance(value, set):
                value = list(value)
            data[key] = value

        data.pop("_record", None)
        report_issue = data.pop("report_issue", True)
        if not report_issue:
            return
        record = {
            "timestamp": datetime_iso(datetime.utcnow()),
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
        out = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        self.fh.write(out)

    def clear(self) -> None:
        """Clear (delete) the issues log file."""
        self.close()
        self.path.unlink(missing_ok=True)
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
        for scope in self.dataset.leaves:
            path = get_dataset_resource(scope, ISSUES_LOG)
            if path is None or not path.is_file():
                continue
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
