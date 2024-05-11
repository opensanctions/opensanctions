import os
import json
import string
import secrets
from typing import Any, List, Iterator, Optional
from datetime import datetime, timezone


class RunID(object):
    """A class to represent a run ID, which consists of a timestamp
    and a random tag."""

    def __init__(self, dt: datetime, tag: str) -> None:
        self.dt: datetime = dt
        self.tag: str = tag

    @classmethod
    def new(cls) -> "RunID":
        now = datetime.now().astimezone(timezone.utc)
        now = now.replace(tzinfo=None)
        now = now.replace(microsecond=0)
        key = [secrets.choice(string.ascii_uppercase) for _ in range(4)]
        return cls(now, "".join(key))

    @classmethod
    def from_string(cls, id: str) -> "RunID":
        if "-" not in id:
            raise ValueError(f"Invalid run ID: {id}")
        ts, tag = id.split("-", 1)
        dt = datetime.strptime(ts, "%Y%m%d%H%M%S")
        dt = dt.replace(tzinfo=None)
        return cls(dt, tag)

    @classmethod
    def from_env(cls, name: str) -> "RunID":
        id = os.environ.get(name)
        if id is None:
            return cls.new()
        return cls.from_string(id)

    @property
    def id(self) -> str:
        return f"{self.dt.strftime('%Y%m%d%H%M%S')}-{self.tag}"

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f"RunID({self.id})"

    def __eq__(self, other: Any) -> bool:
        return self.id == str(other)

    def __hash__(self) -> int:
        return hash(self.id)


class RunHistory(object):
    """A class to represent a history of run IDs."""

    LENGTH = 300

    def __init__(self, items: List[RunID]) -> None:
        self.items = items

    def append(self, run_id: RunID) -> "RunHistory":
        """Creates a new history with the given RunID appended."""
        items = list(self.items)
        items.append(run_id)
        return RunHistory(items[-self.LENGTH :])

    @property
    def latest(self) -> Optional[RunID]:
        if not len(self.items):
            return None
        return self.items[-1]

    def to_json(self) -> str:
        """Return a JSON representation of the run history."""
        items = [str(run) for run in self.items[-self.LENGTH :]]
        return json.dumps({"items": items})

    @classmethod
    def from_json(cls, data: str) -> "RunHistory":
        """Create a run history from a JSON representation."""
        items = json.loads(data).get("items", [])
        items = [RunID.from_string(item) for item in items]
        return cls(items)

    def __iter__(self) -> Iterator[RunID]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)
