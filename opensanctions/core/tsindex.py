import plyvel
from datetime import datetime
from zavod.logs import get_logger
from typing import Iterable, Optional
from nomenklatura.statement import Statement
from nomenklatura.util import datetime_iso, iso_datetime

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
            first_seen = stmt.first_seen
            if isinstance(first_seen, datetime):
                first_seen = datetime_iso(first_seen)
            batch.put(stmt.id.encode("utf-8"), first_seen.encode("utf-8"))
        batch.write()

    def get(self, id: str) -> Optional[datetime]:
        first_seen = self.db.get(id.encode("utf-8"))
        if first_seen is not None:
            return iso_datetime(first_seen.decode("utf-8"))
        return settings.RUN_TIME
