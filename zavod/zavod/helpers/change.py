from lxml import etree
from hashlib import sha1
from typing import Any, Optional
from normality import collapse_spaces

from zavod.logs import get_logger
from zavod.context import Context
from zavod.helpers.xml import ElementOrTree

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


def assert_dom_hash(
    node: Optional[ElementOrTree], hash: str, raise_exc: bool = False
) -> bool:
    """Assert that a DOM node has a given SHA1 hash."""
    digest = sha1()
    if node is not None:
        serialised = etree.tostring(
            node,
            with_comments=False,
            pretty_print=True,
            method="c14n2",
        )
        text = collapse_spaces(serialised.lower())
        if text is not None:
            digest.update(text.replace(" ", "").encode("utf-8"))
    actual = digest.hexdigest()
    if actual != hash:
        if raise_exc:
            msg = f"Expected hash {hash}, got {actual} for {node}"
            raise AssertionError(msg)
        else:
            log.warning(
                "DOM hash changed: %s" % node,
                expected=hash,
                actual=actual,
                node=node,
            )
        return False
    return True
