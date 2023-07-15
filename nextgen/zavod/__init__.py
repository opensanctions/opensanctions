import logging
from pathlib import Path
from typing import Generator, Optional, Type
from contextlib import contextmanager
from nomenklatura.entity import CompositeEntity
from followthemoney.util import PathLike

from zavod import settings
from zavod.context import GenericZavod
from zavod.meta import Dataset
from zavod.logs import configure_logging, get_logger
from zavod.sinks.common import Sink
from zavod.sinks.json_entity import JSONEntitySink

__version__ = "0.7.5"
__all__ = [
    "init",
    "context",
    "Zavod",
    "Dataset",
    "ZD",
    "PathLike",
    "configure_logging",
    "get_logger",
    "settings",
]

logging.getLogger("prefixdate").setLevel(logging.ERROR)


class Zavod(GenericZavod[CompositeEntity]):
    pass


def init(
    metadata_path: PathLike,
    verbose: bool = False,
    data_path: Path = settings.DATA_PATH,
    out_file: Optional[PathLike] = "fragments.json",
    sink: Optional[Sink[CompositeEntity]] = None,
    sink_cls: Type[Sink[CompositeEntity]] = JSONEntitySink,
) -> Zavod:
    """Initiate the zavod working environment and create a processing context."""
    level = logging.DEBUG if verbose else logging.INFO
    configure_logging(level=level)
    if out_file is not None and sink is None:
        out_path = data_path.joinpath(out_file)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        sink = sink_cls(out_path)
    dataset = Dataset.from_path(metadata_path)
    return Zavod(dataset, CompositeEntity, data_path=data_path, sink=sink)


@contextmanager
def init_context(
    metadata_path: PathLike,
    verbose: bool = False,
    data_path: Path = settings.DATA_PATH,
    out_file: Optional[PathLike] = "fragments.json",
    sink: Optional[Sink[CompositeEntity]] = None,
) -> Generator[Zavod, None, None]:
    ctx = init(
        metadata_path,
        verbose=verbose,
        data_path=data_path,
        out_file=out_file,
        sink=sink,
    )
    try:
        yield ctx
    finally:
        ctx.close()
