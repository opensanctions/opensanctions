from typing import BinaryIO
from nomenklatura.entity import CE
from followthemoney.cli.util import write_entity

from zavod.sinks.common import FileSink


class JSONEntitySink(FileSink[CE]):
    def emit_locked(self, fh: BinaryIO, entity: CE) -> None:
        write_entity(fh, entity)

    def __repr__(self) -> str:
        return f"<JSONEntitySink({self.path!r})>"
