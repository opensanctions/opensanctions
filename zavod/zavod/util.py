from lxml import etree
from typing import Optional, Union, IO, Any, Dict
from normality import slugify
import orjson
import logging
import re

log = logging.getLogger(__name__)
ElementOrTree = Union[etree._Element, etree._ElementTree]


def join_slug(
    *parts: Optional[str],
    prefix: Optional[str] = None,
    sep: str = "-",
    strict: bool = True,
    max_len: int = 255
) -> Optional[str]:
    sections = [slugify(p, sep=sep) for p in parts]
    if strict and None in sections:
        return None
    texts = [p for p in sections if p is not None]
    if not len(texts):
        return None
    prefix = slugify(prefix, sep=sep)
    if prefix is not None:
        texts = [prefix, *texts]
    return sep.join(texts)[:max_len].strip(sep)


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


# https://stackoverflow.com/a/49146722/330558
def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)