import math
from collections.abc import Iterator, Mapping
from datetime import date, datetime, time
from pathlib import Path
from hashlib import sha1
from typing import Any

from zavod.logs import get_logger
from zavod.context import Context
from zavod.util import ElementOrTree
from zavod.helpers.xml import xml_node_hash

log = get_logger(__name__)
UNIT_SEP = "\x1f"
RECORD_SEP = "\x1e"


def assert_url_hash(
    context: Context,
    url: str,
    hash: str,
    raise_exc: bool = False,
    auth: Any | None = None,
    headers: Any | None = None,
) -> bool:
    """Assert that a document located at the URL has a given SHA1 hash."""
    digest = sha1()
    with context.http.get(url, auth=auth, headers=headers, stream=True) as res:
        res.raise_for_status()
        for chunk in res.iter_content(chunk_size=8192 * 10):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != hash:
        if raise_exc:
            msg = f"Expected hash {hash}, got {actual} for {url}"
            raise AssertionError(msg)
        else:
            log.warning(
                f"URL hash changed: {url}",
                expected=hash,
                actual=actual,
                url=url,
            )
        return False
    return True


def assert_file_hash(
    path: Path,
    hash: str,
    raise_exc: bool = False,
) -> bool:
    """Assert that a file has a given SHA1 hash."""
    digest = sha1()
    with open(path, "rb") as fh:
        digest.update(fh.read())
    actual = digest.hexdigest()
    if actual != hash:
        if raise_exc:
            msg = f"Expected hash {hash}, got {actual} for {path.name}"
            raise AssertionError(msg)
        else:
            log.warning(
                f"File hash changed: {path.name}",
                expected=hash,
                actual=actual,
                name=path.name,
            )
        return False
    return True


def assert_dom_hash(
    node: ElementOrTree | None,
    hash: str,
    raise_exc: bool = False,
    text_only: bool = False,
) -> bool:
    """Assert that a DOM node has a given SHA1 hash."""
    actual = xml_node_hash(node, text_only=text_only)
    if actual != hash:
        if raise_exc:
            msg = f"Expected hash {hash}, got {actual} for {node!r}"
            raise AssertionError(msg)
        else:
            log.warning(
                f"DOM hash changed: {node}",
                expected=hash,
                actual=actual,
                node=repr(node),
            )
        return False
    return True


def assert_html_url_hash(
    context: Context,
    url: str,
    hash: str,
    path: str | None = None,
    raise_exc: bool = False,
    text_only: bool = False,
) -> bool:
    """Assert that an HTML document located at the URL has a given SHA1 hash."""
    doc = context.fetch_html(url)
    node = doc.find(path) if path is not None else doc
    return assert_dom_hash(node, hash, raise_exc=raise_exc, text_only=text_only)


def _clean_record_text(text: str) -> str:
    """Trim text and drop the separators, so it can't fake a field boundary."""
    return text.replace(UNIT_SEP, "").replace(RECORD_SEP, "").strip()


def _normalise_record(value: Any) -> Any:
    """Normalise a record for hashing; return None for an empty value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        text = _clean_record_text(value)
        return text if len(text) > 0 else None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Cannot hash non-finite float: {value!r}")
        text = f"{value:.2f}".rstrip("0").rstrip(".")
        return "0" if text == "-0" else text
    if isinstance(value, datetime):
        # Spreadsheet readers return date cells as datetimes at midnight.
        if value.time() == time(0) and value.tzinfo is None:
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        normalised: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"Cannot hash record key: {key!r}")
            normalised_item = _normalise_record(item)
            if normalised_item is not None:
                normalised[_clean_record_text(key)] = normalised_item
        return normalised if len(normalised) > 0 else None
    if isinstance(value, (list, tuple)):
        items = [_normalise_record(item) for item in value]
        kept = [item for item in items if item is not None]
        return kept if len(kept) > 0 else None
    raise TypeError(f"Cannot hash record value: {value!r}")


def _record_lines(value: Any, path: tuple[str, ...]) -> Iterator[str]:
    """Flatten a normalised record into one line per value, prefixed by its path."""
    if value is None:
        return
    if isinstance(value, dict):
        for key in sorted(value):
            yield from _record_lines(value[key], (*path, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _record_lines(item, (*path, str(index)))
    else:
        yield UNIT_SEP.join((*path, value)) + RECORD_SEP


def record_hash(record: Any) -> str:
    """Return a stable checksum of a source record, for `h.observe_change`.

    Use this for JSON or tabular sources; use `h.xml_node_hash` for XML. The
    hash evens out structural changes while keeping changes to the content:

    * Dict keys are sorted, and lists and tuples keep their order.
    * Strings are trimmed but keep their case.
    * Numbers become strings, with floats rounded to two decimals (``1.0`` and
      ``1`` hash alike), and bools become ``"true"`` and ``"false"``.
    * Dates and datetimes use ISO format; a naive datetime at midnight hashes
      as its date.
    * ``None``, empty strings and empty containers are dropped, so a dict key
      holding one hashes like a missing key.

    The record is hashed as one line per value: its path of dict keys and list
    positions and the value, joined by the ASCII unit separator and ended by the
    record separator. Both separators are removed from the source text.

    Args:
        record: A dict, list, tuple or scalar from the source.

    Raises:
        TypeError: For values of any other type, or non-string dict keys.
        ValueError: For NaN or infinite floats.
    """
    digest = sha1()
    for line in _record_lines(_normalise_record(record), ()):
        digest.update(line.encode("utf-8"))
    return digest.hexdigest()
