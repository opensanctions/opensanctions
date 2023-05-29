from pathlib import Path
from typing import Generator
from zavod.logs import get_logger
from nomenklatura.statement import Statement, CSV, write_statements

from opensanctions.core.dataset import Dataset
from opensanctions.core.archive import iter_dataset_statements

log = get_logger(__name__)


def dump_statements(dataset: Dataset) -> Generator[Statement, None, None]:
    for idx, stmt in enumerate(iter_dataset_statements(dataset, external=True)):
        if idx > 0 and idx % 50000 == 0:
            log.info("Exporting statements...", idx=idx)
        yield stmt


def export_statements(dataset: Dataset, path: Path) -> None:
    # path = settings.DATASET_PATH.joinpath("statements.csv")
    log.info("Writing global statements list", path=path)
    with open(path, "wb") as fh:
        write_statements(fh, CSV, dump_statements(dataset))
