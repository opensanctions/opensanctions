from nomenklatura.entity import CE
from nomenklatura.statement.serialize import get_statement_writer, JSON, PACK
from followthemoney.util import PathLike

from zavod.sinks.common import FileSink


class JSONStatementSink(FileSink[CE]):
    FORMAT = JSON

    def __init__(self, path: PathLike) -> None:
        super().__init__(path)
        self.fh = open(self.path, "wb")
        self.writer = get_statement_writer(self.fh, self.FORMAT)

    def emit(self, entity: CE) -> None:
        with self.lock:
            for stmt in entity.statements:
                self.writer.write(stmt)

    def close(self) -> None:
        with self.lock:
            self.writer.close()
            super().close()

    def __repr__(self) -> str:
        return f"<JSONStatementSink({self.path!r})>"


class PackStatementSink(JSONStatementSink[CE]):
    FORMAT = PACK
