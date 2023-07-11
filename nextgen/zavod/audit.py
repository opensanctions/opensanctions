from lxml.etree import _Element, tostring
from pprint import pprint, pformat
from typing import Any, Dict, List, Optional
from datapatch import Result

from followthemoney.proxy import EntityProxy


def audit_data(data: Dict[Optional[str], Any], ignore: List[str] = []) -> None:
    """Print a row if any of the fields not ignored are still unused."""
    cleaned = {}
    for key, value in data.items():
        if key in ignore:
            continue
        if value is None or value == "":
            continue
        cleaned[key] = value
    if len(cleaned):
        pprint(cleaned)


def inspect(obj: Any) -> Optional[str]:
    """Deep-view an object for debug purposes."""
    if isinstance(obj, _Element):
        return tostring(obj, encoding="utf-8", pretty_print=True).decode("utf-8")
    if isinstance(obj, EntityProxy):
        obj = obj.to_dict()
    if isinstance(obj, Result):
        obj = repr(obj)
    return pformat(obj)
