import os
import string
import secrets
from typing import Any
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
