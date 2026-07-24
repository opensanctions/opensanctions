import orjson
import logging
from dataclasses import dataclass
from lxml import etree
from functools import cache
from typing import Union, IO, Any
from normality import slugify
from followthemoney.util import ENTITY_ID_LEN

log = logging.getLogger(__name__)
Element = etree._Element
ElementOrTree = Union[etree._Element, etree._ElementTree]
ID_SEP = "-"


@dataclass(frozen=True)
class LangText:
    """A piece of text together with its ISO 639-2 language code.

    Use this as the general-purpose carrier for translated/transliterated
    strings across zavod (e.g. ``shed/trans.py``).
    """

    text: str
    lang: str | None
    """ISO 639-2 (3-letter) language code, or None if not known."""


@cache
def slugify_prefix(prefix: str | None) -> str | None:
    return slugify(prefix, sep=ID_SEP)


def join_slug(
    *parts: str | None,
    prefix: str | None = None,
    strict: bool = True,
) -> str | None:
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


def write_json(data: dict[str, Any], fh: IO[bytes]) -> None:
    """Write a JSON object to the given open file handle."""
    opt = orjson.OPT_APPEND_NEWLINE | orjson.OPT_NON_STR_KEYS
    fh.write(orjson.dumps(data, option=opt, default=json_default))
