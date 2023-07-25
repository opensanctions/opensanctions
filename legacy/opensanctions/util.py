import orjson
import logging
from typing import IO, Any, Dict

log = logging.getLogger(__name__)


def json_default(obj: Any) -> Any:
    if isinstance(obj, (tuple, set)):
        return list(obj)
    try:
        return obj.to_dict()
    except AttributeError:
        pass
    raise TypeError


def write_json(data: Dict[str, Any], fh: IO[bytes]) -> None:
    """Write a JSON object to the given open file handle."""
    opt = orjson.OPT_APPEND_NEWLINE | orjson.OPT_NON_STR_KEYS
    fh.write(orjson.dumps(data, option=opt, default=json_default))
