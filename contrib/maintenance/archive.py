"""Read run state from the data.opensanctions.org archive.

Archive semantics this module encodes:

* `artifacts/{name}/versions.json` holds `{"items": [...], "last_successful"}`;
  `items[-1]` is the most recent run, successful or not.
* Version IDs are `YYYYMMDDHHMMSS-xxx` — parseable run timestamps.
* Failed runs still archive `index.json`, `issues.json` and `issues.log`, but
  `statistics.json`, `statements.pack`, `resources.json` and `delta.json` are
  only present for successful runs.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from . import session

ARCHIVE_SITE = "https://data.opensanctions.org"
CATALOG_URL = f"{ARCHIVE_SITE}/datasets/latest/index.json"

# The artifacts a run may leave behind, in the order we report them.
RUN_ARTIFACTS = [
    "index.json",
    "issues.json",
    "issues.log",
    "statistics.json",
    "statements.pack",
    "resources.json",
    "delta.json",
]


def artifact_url(dataset_name: str, version_id: str, resource: str) -> str:
    return f"{ARCHIVE_SITE}/artifacts/{dataset_name}/{version_id}/{resource}"


def version_timestamp(version_id: str) -> datetime | None:
    """Parse the run timestamp encoded in a version ID like `20260709103756-gpp`."""
    try:
        stamp = version_id.split("-", 1)[0]
        return datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@dataclass
class VersionsInfo:
    """The run history of a dataset, as read from versions.json."""

    items: list[str]
    last_successful: str | None

    @property
    def latest(self) -> str | None:
        return self.items[-1] if self.items else None

    @property
    def latest_failed(self) -> bool:
        return self.latest is not None and self.latest != self.last_successful

    def runs_since_success(self) -> list[str]:
        """Version IDs of the consecutive failed runs after the last success.

        When the last success is older than the (capped, 100-entry) history
        page, every listed run is failed and the true count is even higher.
        """
        if self.last_successful in self.items:
            return self.items[self.items.index(self.last_successful) + 1 :]
        return list(self.items)


def get_versions(dataset_name: str) -> VersionsInfo | None:
    """Fetch a dataset's run history; None if it was never deployed."""
    url = f"{ARCHIVE_SITE}/artifacts/{dataset_name}/versions.json"
    response = session.get(url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json()
    return VersionsInfo(
        items=list(data.get("items", [])),
        last_successful=data.get("last_successful"),
    )


def head_artifact(
    dataset_name: str, version_id: str, resource: str
) -> tuple[int, int | None]:
    """HEAD-check one artifact; returns (status_code, content_length).

    Asks for identity encoding — with gzip the CDN omits Content-Length.
    """
    response = session.head(
        artifact_url(dataset_name, version_id, resource),
        headers={"Accept-Encoding": "identity"},
    )
    length = response.headers.get("Content-Length")
    return response.status_code, int(length) if length is not None else None


def fetch_json_artifact(
    dataset_name: str, version_id: str, resource: str
) -> Any | None:
    """Fetch and parse a JSON artifact; None when absent or unparseable."""
    response = session.get(artifact_url(dataset_name, version_id, resource))
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except json.JSONDecodeError:
        return None


def get_issues(dataset_name: str, version_id: str) -> list[dict[str, Any]] | None:
    """Fetch the issues of one run; None when the artifact is missing."""
    data = fetch_json_artifact(dataset_name, version_id, "issues.json")
    if data is None:
        return None
    issues = data.get("issues", [])
    assert isinstance(issues, list), f"Unexpected issues.json shape: {dataset_name}"
    return issues


def get_issue_details(issues_url: str) -> Any | None:
    """Fetch an issues.json by URL (as listed in the catalog index)."""
    response = session.get(issues_url)
    if response.status_code != 200:
        return None
    try:
        return response.json()
    except json.JSONDecodeError:
        return None


def get_catalog() -> Any:
    """Fetch the full latest-datasets catalog (large; call once per process)."""
    response = session.get(CATALOG_URL)
    response.raise_for_status()
    return response.json()
