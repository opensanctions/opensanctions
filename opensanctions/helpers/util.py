from pprint import pprint
from typing import Any, Dict, List


def audit_data(data: Dict[str, Any], ignore: List[str] = []) -> None:
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
