from typing import BinaryIO, Optional
from nomenklatura.statement import Statement
from nomenklatura.statement.serialize import get_statement_writer, StatementWriter, PACK


from zavod.meta import Dataset
from zavod.archive import dataset_resource_path, STATEMENTS_RESOURCE


class DatasetSink(object):
    """Manage a file handle for writing statements to a dataset archive path."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.path = dataset_resource_path(dataset.name, STATEMENTS_RESOURCE)
        self.fh: Optional[BinaryIO] = None
        self.writer: Optional[StatementWriter] = None

    def emit(self, stmt: Statement) -> None:
        if self.fh is None or self.writer is None:
            self.fh = open(self.path, "wb")
            self.writer = get_statement_writer(self.fh, PACK)
        self.writer.write(stmt)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        if self.fh is not None:
            self.fh.close()
            self.fh = None

    def clear(self) -> None:
        self.close()
        if self.path.is_file():
            self.path.unlink()
