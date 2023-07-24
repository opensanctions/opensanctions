from lxml import etree
from typing import Optional, Union
from normality import slugify


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
