from lxml.etree import _Element, tostring
from pprint import pformat
from typing import Any, Optional
from datapatch import Result

from followthemoney.proxy import EntityProxy


def inspect(obj: Any) -> Optional[str]:
    """Deep-view an object for debug purposes."""
    if isinstance(obj, _Element):
        return tostring(obj, encoding="utf-8", pretty_print=True).decode("utf-8")
    if isinstance(obj, EntityProxy):
        obj = obj.to_dict()
    if isinstance(obj, Result):
        obj = repr(obj)
    return pformat(obj)
