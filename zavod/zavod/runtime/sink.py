from typing import Optional, TextIO
from normality.encoding import DEFAULT_ENCODING
from followthemoney import Statement
from followthemoney.statement.serialize import PackStatementWriter

from zavod.meta import Dataset
from zavod.archive import dataset_resource_path, STATEMENTS_FILE


class DatasetSink(object):
    """Manage a file handle for writing statements to a dataset archive path."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.path = dataset_resource_path(dataset.name, STATEMENTS_FILE)
        self.fh: Optional[TextIO] = None
        self.writer: Optional[PackStatementWriter] = None

    def emit(self, stmt: Statement) -> None:
        """Write a statement to the dataset output."""
        if self.fh is None or self.writer is None:
            self.fh = open(self.path, "w", encoding=DEFAULT_ENCODING)
            self.writer = PackStatementWriter(self.fh)
        self.writer.write(stmt)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        if self.fh is not None:
            self.fh.close()
            self.fh = None

    def clear(self) -> None:
        """Delete the dataset statements output file."""
        self.close()
        if self.path.is_file():
            self.path.unlink()
