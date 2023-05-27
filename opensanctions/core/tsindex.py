import plyvel
from zavod.logs import get_logger
from typing import Iterable
from nomenklatura.statement import Statement

from opensanctions import settings

log = get_logger(__name__)


class TimeStampIndex(object):
    def __init__(self) -> None:
        path = settings.DATA_PATH / "timestamps"
        self.db = plyvel.DB(path.as_posix(), create_if_missing=True)

    def index(self, statements: Iterable[Statement]) -> None:
        log.info("Building timestamp index...")
        batch = self.db.write_batch()
        for stmt in statements:
            if stmt.first_seen is not None:
                batch.put(stmt.id.encode("utf-8"), stmt.first_seen.encode("utf-8"))
        batch.write()

    def get(self, id: str) -> str:
        first_seen = self.db.get(id.encode("utf-8"))
        if first_seen is not None:
            return first_seen.decode("utf-8")
        return settings.RUN_TIME_ISO
