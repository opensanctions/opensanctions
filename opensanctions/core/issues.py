import orjson
import logging
from pathlib import Path
from banal import is_mapping
from datetime import datetime
from lxml.etree import _Element, tostring
from typing import TYPE_CHECKING
from typing import Any, Dict, Generator, Optional, TypedDict, cast
from followthemoney.schema import Schema

from opensanctions import settings
from opensanctions.core.archive import dataset_resource_path, get_dataset_resource
from opensanctions.core.archive import ISSUES_LOG_RESOURCE
from opensanctions.core.dataset import Dataset

if TYPE_CHECKING:
    from opensanctions.core.context import Context


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


class IssueWriter(object):
    def __init__(self, dataset: Dataset) -> None:
        self.path = dataset_resource_path(dataset, ISSUES_LOG_RESOURCE)
        self.fh = open(self.path, "ab")

    def clear(self) -> None:
        self.fh.close()
        self.fh = open(self.path, "wb")

    def write(self, event: Dict[str, Any]) -> None:
        data = dict(event)
        for key, value in data.items():
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
            "timestamp": data.pop("timestamp", None),
            "module": data.pop("logger", None),
            "level": data.pop("level"),
            "message": data.pop("event", None),
            "dataset": data.pop("dataset"),
        }
        entity = data.pop("entity", None)
        if is_mapping(entity):
            record["entity"] = entity
        elif isinstance(entity, str):
            record["entity"] = {"id": entity}
        record["data"] = data
        out = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        self.fh.write(out)

    def close(self) -> None:
        self.fh.close()


def store_log_event(logger, log_method, data: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in data.items():
        if isinstance(value, _Element):
            value = tostring(value, pretty_print=False, encoding=str)
        if isinstance(value, Path):
            value = str(value.relative_to(settings.DATA_PATH))
        if isinstance(value, Schema):
            value = value.name
        data[key] = value

    context: Optional[Context] = data.pop("_context", None)
    dataset = data.get("dataset", None)
    level = data.get("level")
    if level is not None:
        level_num = getattr(logging, level.upper())
        if level_num > logging.INFO and dataset is not None:
            if context is not None:
                context.issues.write(data)
    return data


def _all_issues(dataset: Dataset) -> Generator[Issue, None, None]:
    path = get_dataset_resource(dataset, ISSUES_LOG_RESOURCE)
    if path is None or not path.is_file():
        return
    with open(path, "rb") as fh:
        for line in fh:
            yield cast(Issue, orjson.loads(line))


def all_issues(dataset: Dataset) -> Generator[Issue, None, None]:
    for scope in dataset.scopes:
        yield from _all_issues(scope)


def agg_issues_by_level(dataset: Dataset) -> Dict[str, int]:
    levels: Dict[str, int] = {}
    for issue in all_issues(dataset):
        levels[issue.get("level")] = levels.get(issue.get("level"), 0) + 1
    return levels
