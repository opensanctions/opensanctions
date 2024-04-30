from typing import Any, Callable, Dict, Optional

from lxml import html, etree
from time import sleep

from zavod import settings
from zavod.context import Context
from zavod.http import request_hash


class UnblockFailedException(RuntimeError):
    def __init__(self, url: str):
        super().__init__(f"Unblocking failed for URL: {url}")


def fetch_html(
    context: Context,
    url: str,
    unblock_validator: Callable[[etree._Element], bool],
    actions: list[Dict[str, Any]] = [],
    javascript: Optional[bool] = None,
    cache_days: Optional[int] = None,
    fingerprint: Optional[str] = None,
    retries: int = 3,
    backoff_factor: int = 3,
    previous_retries: int = 0,
) -> etree._Element:
    """
    Fetch a web page using the Zyte API.

    Args:
        context: The context object.
        url: The URL of the web page.
        unblock_validator: A function that checks if the page is unblocked
            successfully. This is important to ensure we don't cache pages
            that weren't actually unblocked successfully.
        javascript: Whether to execute JavaScript on the page.
        cache_days: The number of days to cache the page.
        fingerprint: The cache key for this request, if customisation is needed.
        retries: The number of times to retry if unblocking fails.
        backoff_factor: Factor to scale the pause between retries.

    Returns:
        The parsed HTML document serialized from the DOM.
    """
    if settings.ZYTE_API_KEY is None:
        raise RuntimeError("ZYTE_API_KEY is not set")

    zyte_data = {
        "browserHtml": True,
        "actions": actions,
    }
    if javascript is not None:
        zyte_data["javascript"] = javascript

    if fingerprint is None:
        # Slight abuse of the cache key to produce keys of the usual style.
        # Technically this isn't the data that's sent
        # to the target server (url), but technically we're not caching the response
        # from Zyte API either, we're caching its HTML contents.
        fingerprint = request_hash(url, data=zyte_data)
    if cache_days is not None:
        text = context.cache.get(fingerprint, max_age=cache_days)
        if text is not None:
            context.log.debug("HTTP cache hit", url=url, fingerprint=fingerprint)
            try:
                return html.fromstring(text)
            except Exception:
                context.clear_url(fingerprint)
                raise

    context.log.debug(f"Zyte API request: {url}", data=zyte_data)
    zyte_data["url"] = url
    api_response = context.http.post(
        "https://api.zyte.com/v1/extract",
        auth=(settings.ZYTE_API_KEY, ""),
        json=zyte_data,
    )
    api_response.raise_for_status()
    text = api_response.json()["browserHtml"]
    assert text is not None
    doc = html.fromstring(text)

    if not unblock_validator(doc):
        if retries > 0:
            pause = backoff_factor * (2 ** (previous_retries + 1))
            context.log.debug(
                f"Unblocking failed, sleeping {pause}s then retrying",
                url=url,
                retries=retries,
            )
            sleep(pause)
            return fetch_html(
                context,
                url,
                unblock_validator,
                actions,
                javascript=javascript,
                cache_days=cache_days,
                retries=retries - 1,
                backoff_factor=backoff_factor,
                previous_retries=previous_retries + 1,
            )
        context.log.debug("Unblocking failed", url=url, html=text)
        raise UnblockFailedException(url)

    if cache_days is not None:
        context.cache.set(fingerprint, text)
    return doc
