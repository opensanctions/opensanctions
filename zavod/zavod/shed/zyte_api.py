from pathlib import Path
from lxml import html, etree
from time import sleep
from base64 import b64decode
from typing import Any, Dict, List, Optional, Tuple
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from email.message import Message
import json

from zavod import settings
from zavod.archive import dataset_data_path
from zavod.context import Context
from zavod.runtime.http_ import request_hash


ZYTE_API_URL = "https://api.zyte.com/v1/extract"


class UnblockFailedException(RuntimeError):
    def __init__(self, url: str, validator: str):
        super().__init__(
            f"Unblocking failed for URL: '{url}' with validator: '{validator}'"
        )


def get_content_type(headers: List[Dict[str, str]]) -> Tuple[str, str | None]:
    header = [h["value"] for h in headers if h["name"].lower() == "content-type"][0]
    # I kid you not, this is the https://peps.python.org/pep-0594/#cgi recommended
    # way to replace cgi.parse_header
    message = Message()
    message["Content-Type"] = header
    charset = message.get_param("charset")
    charset = charset.lower() if isinstance(charset, str) else None
    media_type = message.get_content_type().lower()

    assert charset is None or isinstance(charset, str), header
    return media_type, charset


def configure_session(session: Session) -> None:
    zyte_retries = Retry(
        total=10,
        backoff_factor=3,
        status_forcelist=list(Retry.RETRY_AFTER_STATUS_CODES) + [520],
        allowed_methods=["POST"],
    )
    session.mount(ZYTE_API_URL, HTTPAdapter(max_retries=zyte_retries))


def fetch_resource(
    context: Context,
    filename: str,
    url: str,
    expected_media_type: Optional[str] = None,
    expected_charset: Optional[str] = None,
    geolocation: Optional[str] = None,
) -> Tuple[bool, str | None, str | None, Path]:
    """
    Fetch a resource using Zyte API and save to filesystem.

    The content type and charset can be used to assert expected
    types (and successful unblocking by Zyte) and for appropriate
    text decoding when the encoding can vary. Do so via the expected_
    arguments unless more logic is required.

    Args:
        context: The context object.
        filename: The name to use when saving the file.
        url: The URL of the resource.
        expected_media_type: If set, assert that the media type in the
            response content-type header matches this value. Not enforced
            when the file already exists locally.
        expected_charset: If set, assert that the charset in the response
            content-type header matches this value. Not enforced
            when the file already exists locally.

    Returns:
        A tuple of:
            - A boolean indicating whether the file was cached.
            - The path to the saved file.
            - The media type of the response, None if cached.
            - The charset of the response, None if cached.
    """
    data_path = dataset_data_path(context.dataset.name)
    out_path = data_path.joinpath(filename)
    if out_path.exists():
        return True, None, None, out_path

    if settings.ZYTE_API_KEY is None:
        raise RuntimeError("OPENSANCTIONS_ZYTE_API_KEY is not set")

    context.log.info("Fetching file", url=url)
    zyte_data: Dict[str, Any] = {
        "httpResponseBody": True,
        "httpResponseHeaders": True,
    }
    if geolocation is not None:
        zyte_data["geolocation"] = geolocation
    context.log.debug(f"Zyte API request: {url}", data=zyte_data)
    zyte_data["url"] = url
    out_path.parent.mkdir(parents=True, exist_ok=True)
    configure_session(context.http)

    api_response = context.http.post(
        ZYTE_API_URL,
        auth=(settings.ZYTE_API_KEY, ""),
        json=zyte_data,
    )
    api_response.raise_for_status()

    file_base64 = api_response.json()["httpResponseBody"]
    with open(out_path, "wb") as fh:
        fh.write(b64decode(file_base64))
    media_type, charset = get_content_type(api_response.json()["httpResponseHeaders"])

    if expected_media_type:
        assert media_type == expected_media_type, (media_type, charset, url)
    if expected_charset:
        assert charset == expected_charset, (media_type, charset, url)

    return False, media_type, charset, out_path


def fetch_text(
    context: Context,
    url: str,
    headers: List[Dict[str, str]] = [],
    cache_days: Optional[int] = None,
    expected_media_type: Optional[str] = None,
    expected_charset: Optional[str] = None,
    geolocation: Optional[str] = None,
) -> Tuple[bool, str | None, str | None, str]:
    """
    Fetch a text document using the Zyte API.

    The content type and charset can be used to assert expected
    types (and successful unblocking by Zyte) and for appropriate
    text decoding when the encoding can vary. Do so via the expected_
    arguments unless more logic is required.

    Args:
        context: The context object.
        url: The URL of the text document.
        headers: A list of dicts of headers to send with the request.
        expected_media_type: If set, assert that the media type in the
            response content-type header matches this value.
        expected_charset: If set, assert that the charset in the response
            content-type header matches this value.

    Returns:
        A tuple of:
            - A boolean indicating whether the text was cached.
            - The media type of the response, None if cached.
            - The charset of the response, None if cached.
            - The text content.
    """
    if settings.ZYTE_API_KEY is None:
        raise RuntimeError("OPENSANCTIONS_ZYTE_API_KEY is not set")

    zyte_data: Dict[str, Any] = {
        "httpResponseBody": True,
        "httpResponseHeaders": True,
        "customHttpRequestHeaders": headers,
    }
    if geolocation is not None:
        zyte_data["geolocation"] = geolocation

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
                return True, None, None, text
            except Exception:
                context.clear_url(fingerprint)
                raise

    context.log.debug(f"Zyte API request: {url}", data=zyte_data)
    zyte_data["url"] = url
    configure_session(context.http)

    api_response = context.http.post(
        ZYTE_API_URL,
        auth=(settings.ZYTE_API_KEY, ""),
        json=zyte_data,
    )
    api_response.raise_for_status()

    media_type, charset = get_content_type(api_response.json()["httpResponseHeaders"])
    text_b64 = api_response.json()["httpResponseBody"]
    assert text_b64 is not None
    bytes = b64decode(text_b64)
    if charset is None:
        text = bytes.decode()
    else:
        text = bytes.decode(charset)

    if expected_media_type:
        assert media_type == expected_media_type, (media_type, charset, text)
    if expected_charset:
        assert charset == expected_charset, (media_type, charset, text)

    if cache_days is not None:
        context.cache.set(fingerprint, text)

    return False, media_type, charset, text


def fetch_json(
    context: Context,
    url: str,
    cache_days: Optional[int] = None,
    expected_media_type: Optional[str] = "application/json",
    expected_charset: Optional[str] = "utf-8",
    geolocation: Optional[str] = None,
) -> Any:
    headers = [{"name": "Accept", "value": "application/json"}]
    _, _, _, text = fetch_text(
        context,
        url,
        headers=headers,
        cache_days=cache_days,
        expected_media_type=expected_media_type,
        expected_charset=expected_charset,
        geolocation=geolocation,
    )
    return json.loads(text)


def fetch_html(
    context: Context,
    url: str,
    unblock_validator: str,
    actions: list[Dict[str, Any]] = [],
    html_source: str = "browserHtml",
    javascript: Optional[bool] = None,
    geolocation: Optional[str] = None,
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
        unblock_validator: XPath matching at least one element if and only if
            unblocking was successful. This is important to ensure we don't cache
            pages that weren't actually unblocked successfully.
        actions: A list of dicts of actions to attempt on a rendered page.
        html_source: browserHtml | httpResponseBody
        javascript: Whether to execute JavaScript on the page.
        cache_days: The number of days to cache the page.
        fingerprint: The cache key for this request, if customisation is needed.
        retries: The number of times to retry if unblocking fails.
        backoff_factor: Factor to scale the pause between retries.

    Returns:
        The parsed HTML document serialized from the DOM.
    """
    if settings.ZYTE_API_KEY is None:
        raise RuntimeError("OPENSANCTIONS_ZYTE_API_KEY is not set")

    zyte_data = {
        "actions": actions,
        html_source: True,
        "httpResponseHeaders": True,
    }
    if javascript is not None:
        zyte_data["javascript"] = javascript
    if geolocation is not None:
        zyte_data["geolocation"] = geolocation

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
    configure_session(context.http)
    api_response = context.http.post(
        ZYTE_API_URL,
        auth=(settings.ZYTE_API_KEY, ""),
        json=zyte_data,
    )
    api_response.raise_for_status()
    for action in api_response.json().get("actions", []):
        context.log.info("Zyte action result", **action)

    text = api_response.json()[html_source]
    assert text is not None
    if html_source == "httpResponseBody":
        media_type, charset = get_content_type(
            api_response.json()["httpResponseHeaders"]
        )
        assert charset is not None, zyte_data
        text = b64decode(text).decode(charset)
    doc = html.fromstring(text)

    matches = doc.xpath(unblock_validator)
    if not isinstance(matches, list) or not len(matches) > 0:
        if previous_retries < retries:
            pause = backoff_factor * (2 ** (previous_retries + 1))
            context.log.debug(
                f"Unblocking failed, sleeping {pause}s then retrying",
                url=url,
                retries=retries,
                previous_retries=previous_retries,
            )
            sleep(pause)
            return fetch_html(
                context,
                url,
                unblock_validator,
                actions,
                html_source=html_source,
                javascript=javascript,
                cache_days=cache_days,
                retries=retries,
                backoff_factor=backoff_factor,
                previous_retries=previous_retries + 1,
            )
        context.log.debug("Unblocking failed", url=url, html=text)
        raise UnblockFailedException(url, unblock_validator)

    if cache_days is not None:
        context.cache.set(fingerprint, text)
    return doc
