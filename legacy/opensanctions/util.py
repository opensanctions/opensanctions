import orjson
import logging
from banal import ensure_list
from typing import IO, Any, Dict

log = logging.getLogger(__name__)


def multi_split(text, splitters):
    """Sequentially attempt to split a text based on an array of splitting criteria.
    This is useful for strings where multiple separators are used to separate values,
    e.g.: `test,other/misc`. A special case of this is itemised lists like `a) test
    b) other c) misc` which sanction-makers seem to love."""
    fragments = ensure_list(text)
    for splitter in splitters:
        out = []
        for fragment in fragments:
            for frag in fragment.split(splitter):
                frag = frag.strip()
                if len(frag):
                    out.append(frag)
        fragments = out
    return fragments


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
