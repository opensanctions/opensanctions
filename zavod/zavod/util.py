import re
import orjson
import logging
from lxml import etree
from functools import cache
from typing import Optional, Union, IO, Any, Dict
from normality import slugify
from followthemoney.util import ENTITY_ID_LEN

log = logging.getLogger(__name__)
ElementOrTree = Union[etree._Element, etree._ElementTree]
ID_SEP = "-"


@cache
def slugify_prefix(prefix: Optional[str]) -> Optional[str]:
    return slugify(prefix, sep=ID_SEP)


def join_slug(
    *parts: Optional[str],
    prefix: Optional[str] = None,
    strict: bool = True,
) -> Optional[str]:
    """Make a text-based ID which is strongly normalized."""
    sections = [slugify(p, sep=ID_SEP) for p in parts]
    if strict and None in sections:
        return None
    texts = [p for p in sections if p is not None]
    if not len(texts):
        return None
    prefix = slugify_prefix(prefix)
    if prefix is not None:
        texts = [prefix, *texts]
    return ID_SEP.join(texts)[:ENTITY_ID_LEN].strip(ID_SEP)


def prefixed_hash_id(prefix: str, hash: str) -> str:
    """Make a hash-based ID with a prefix."""
    slug_prefix = slugify_prefix(prefix)
    assert slug_prefix is not None, "Invalid prefix"
    return f"{slug_prefix}-{hash}"[:ENTITY_ID_LEN]


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
def remove_emoji(string: str) -> str:
    emoji_pattern = re.compile(
        "["
        "\U0001f600-\U0001f64f"  # emoticons
        "\U0001f300-\U0001f5ff"  # symbols & pictographs
        "\U0001f680-\U0001f6ff"  # transport & map symbols
        "\U0001f1e0-\U0001f1ff"  # flags (iOS)
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", string)
