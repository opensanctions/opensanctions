"""Read run state from the data.opensanctions.org archive.

Archive semantics this module encodes:

* `artifacts/{name}/versions.json` holds `{"items": [...], "last_successful"}`;
  `items[-1]` is the most recent run, successful or not.
* Version IDs are `YYYYMMDDHHMMSS-xxx` — parseable run timestamps.
* Failed runs still archive `index.json`, `issues.json` and `issues.log`, but
  `statistics.json`, `statements.pack`, `resources.json` and `delta.json` are
  only present for successful runs.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
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
# The subset archived even when the run failed; the rest of RUN_ARTIFACTS is
# only ever exported by successful runs.
FAILED_RUN_ARTIFACTS = ["index.json", "issues.json", "issues.log"]


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


@lru_cache(maxsize=1024)
def fetch_json(url: str) -> Any | None:
    """GET and parse a JSON document, memoized per URL for the process lifetime.

    The issues agent and the diagnose report both read a dataset's
    versions.json and issues.json; memoizing spares the duplicate round-trips
    and guarantees both describe the same run even if a new one publishes
    mid-process. Version-prefixed artifact URLs are immutable, so entries
    never go stale; versions.json can, but minutes-level staleness within one
    process is deliberate. Treat results as read-only — every caller gets the
    same object.

    Returns None on 404 (cached: absent stays absent). Other HTTP errors and
    unparseable payloads raise, and are not cached, so a retry gets a fresh
    attempt.
    """
    response = session.get(url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def get_versions(dataset_name: str) -> VersionsInfo | None:
    """Fetch a dataset's run history; None if it was never deployed."""
    data = fetch_json(f"{ARCHIVE_SITE}/artifacts/{dataset_name}/versions.json")
    if data is None:
        return None
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


def fetch_artifact(dataset_name: str, version_id: str, resource: str) -> Any | None:
    """Fetch and parse one run's JSON artifact; None when absent (cached)."""
    return fetch_json(artifact_url(dataset_name, version_id, resource))


def get_issues(dataset_name: str, version_id: str) -> list[dict[str, Any]] | None:
    """Fetch the issues of one run; None when the artifact is missing."""
    data = fetch_artifact(dataset_name, version_id, "issues.json")
    if data is None:
        return None
    issues = data.get("issues", [])
    assert isinstance(issues, list), f"Unexpected issues.json shape: {dataset_name}"
    return issues


def get_catalog() -> Any:
    """Fetch the full latest-datasets catalog.

    Not routed through fetch_json: it is ~20MB parsed and read once, so
    pinning it in the LRU buys nothing, and a missing catalog is a hard error
    rather than a None.
    """
    response = session.get(CATALOG_URL)
    response.raise_for_status()
    return response.json()
