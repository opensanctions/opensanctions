import logging
from followthemoney.util import PathLike

from zavod import settings
from zavod.meta import Dataset
from zavod.logs import configure_logging, get_logger

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
