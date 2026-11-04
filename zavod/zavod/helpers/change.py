from pathlib import Path
from hashlib import sha1
from typing import Any

from zavod.logs import get_logger
from zavod.context import Context
from zavod.util import ElementOrTree
from zavod.helpers.xml import xml_node_hash

log = get_logger(__name__)


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
