from lxml import etree
from hashlib import sha1
from typing import Any, Optional
from normality import collapse_spaces

from zavod.logs import get_logger
from zavod.context import Context
from zavod.util import ElementOrTree

log = get_logger(__name__)


def assert_url_hash(
    context: Context,
    url: str,
    hash: str,
    raise_exc: bool = False,
    auth: Optional[Any] = None,
    headers: Optional[Any] = None,
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
                "URL hash changed: %s" % url,
                expected=hash,
                actual=actual,
                url=url,
            )
        return False
    return True


def _compute_node_hash(
    node: Optional[ElementOrTree], text_only: bool = False
) -> Optional[str]:
    digest = sha1()
    if node is None:
        return None
    if text_only:
        serialised = etree.tostring(
            node,
            method="text",
            encoding="utf-8",
        )
    else:
        serialised = etree.tostring(
            node,
            with_comments=False,
            pretty_print=True,
            method="c14n2",
        )
    text = collapse_spaces(serialised.decode("utf-8").lower())
    if text is None:
        return None
    digest.update(text.replace(" ", "").encode("utf-8"))
    return digest.hexdigest()


def assert_dom_hash(
    node: Optional[ElementOrTree],
    hash: str,
    raise_exc: bool = False,
    text_only: bool = False,
) -> bool:
    """Assert that a DOM node has a given SHA1 hash."""
    actual = _compute_node_hash(node, text_only=text_only)
    if actual != hash:
        if raise_exc:
            msg = f"Expected hash {hash}, got {actual} for {node!r}"
            raise AssertionError(msg)
        else:
            log.warning(
                "DOM hash changed: %s" % node,
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
    path: Optional[str] = None,
    raise_exc: bool = False,
    text_only: bool = False,
) -> bool:
    """Assert that an HTML document located at the URL has a given SHA1 hash."""
    doc = context.fetch_html(url)
    node = doc.find(path) if path is not None else doc
    return assert_dom_hash(node, hash, raise_exc=raise_exc, text_only=text_only)
