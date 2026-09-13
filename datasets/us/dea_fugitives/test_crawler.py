from unittest.mock import Mock

import pytest
from lxml import etree

from . import crawler
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest, ZyteResult, ZyteScrapeType

BASE = "https://www.dea.gov"
SITEMAP_URL = BASE + "/sitemap.xml"
PARTS = [f"{BASE}/sitemap.xml?page={i}" for i in (1, 2)]
PROFILES = [f"{BASE}/fugitives/test-person-{i:04d}" for i in range(400)]


def sitemap(urls: list[str], index: bool = False) -> str:
    root = etree.Element(crawler.SITEMAP_NS + ("sitemapindex" if index else "urlset"))
    for url in urls:
        item = etree.SubElement(
            root, crawler.SITEMAP_NS + ("sitemap" if index else "url")
        )
        etree.SubElement(item, crawler.SITEMAP_NS + "loc").text = url
    return etree.tostring(root, encoding="unicode")


def response(content: str, *, cached: bool = False, status: int = 200) -> ZyteResult:
    return ZyteResult(content, None if cached else status, "cache-key", cached)


def test_complete_discovery_before_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    documents = {
        SITEMAP_URL: sitemap(PARTS, index=True),
        PARTS[0]: sitemap(PROFILES[:200] + [BASE + "/fugitives/all", BASE + "/news"]),
        PARTS[1]: sitemap(PROFILES[199:]),  # Deduplicate across partitions.
    }
    fetched: list[str] = []
    visited: list[str] = []

    def fetch(context: Mock, request: ZyteAPIRequest, cache_days: int) -> ZyteResult:
        assert request.scrape_type == ZyteScrapeType.HTTP_RESPONSE_BODY
        assert cache_days == 1
        fetched.append(request.url)
        return response(documents[request.url])

    def item(url: str, context: Mock) -> None:
        assert fetched == [SITEMAP_URL, *PARTS]
        visited.append(url)

    monkeypatch.setattr(zyte_api, "fetch", fetch)
    monkeypatch.setattr(crawler, "crawl_item", item)
    crawler.crawl(Mock(data_url=SITEMAP_URL))
    assert visited == PROFILES


def test_failed_later_partition_emits_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    fetch = Mock(
        side_effect=[
            response(sitemap(PARTS, True)),
            response(sitemap(PROFILES)),
            RuntimeError("partition unavailable"),
        ]
    )
    item = Mock()
    monkeypatch.setattr(zyte_api, "fetch", fetch)
    monkeypatch.setattr(crawler, "crawl_item", item)
    with pytest.raises(RuntimeError, match="partition unavailable"):
        crawler.crawl(Mock(data_url=SITEMAP_URL))
    item.assert_not_called()


@pytest.mark.parametrize("count", [0, 399, 1201])
def test_profile_count_bounds(monkeypatch: pytest.MonkeyPatch, count: int) -> None:
    urls = [f"{BASE}/fugitives/test-{i}" for i in range(count)] + [BASE + "/news"]
    monkeypatch.setattr(
        zyte_api,
        "fetch",
        Mock(side_effect=[response(sitemap(PARTS[:1], True)), response(sitemap(urls))]),
    )
    item = Mock()
    monkeypatch.setattr(crawler, "crawl_item", item)
    with pytest.raises(ValueError, match="profile count"):
        crawler.crawl(Mock(data_url=SITEMAP_URL))
    item.assert_not_called()


@pytest.mark.parametrize(
    "url",
    [
        "http://www.dea.gov/sitemap.xml",
        "https://example.org/sitemap.xml",
        "https://www.dea.gov@evil.example/sitemap.xml",
        "https://www.dea.gov:443/sitemap.xml",
        "/sitemap.xml",
        BASE + "/sitemap.xml#fragment",
        BASE + "/sitemap.xml?x=bad value",
    ],
)
def test_unsafe_partition_rejected(monkeypatch: pytest.MonkeyPatch, url: str) -> None:
    fetch = Mock(return_value=response(sitemap([url], True)))
    monkeypatch.setattr(zyte_api, "fetch", fetch)
    with pytest.raises(ValueError, match="same-origin"):
        crawler.discover_profiles(Mock(data_url=SITEMAP_URL))
    assert fetch.call_count == 1


@pytest.mark.parametrize(
    "url",
    [
        BASE + "/fugitives/test?x=1",
        BASE + "/fugitives/test/extra",
        BASE + "/fugitives/%2e%2e",
    ],
)
def test_invalid_profile_path(monkeypatch: pytest.MonkeyPatch, url: str) -> None:
    monkeypatch.setattr(
        zyte_api,
        "fetch",
        Mock(
            side_effect=[
                response(sitemap(PARTS[:1], True)),
                response(sitemap(PROFILES + [url])),
            ]
        ),
    )
    with pytest.raises(ValueError, match="profile URL"):
        crawler.discover_profiles(Mock(data_url=SITEMAP_URL))


@pytest.mark.parametrize(
    "document",
    [
        "<html><h1>Challenge</h1></html>",
        "<broken",
        "",
        '<!DOCTYPE x [<!ENTITY x SYSTEM "file:///etc/passwd">]><x>&x;</x>',
        '<!DOCTYPE x SYSTEM "https://example.org/external.dtd"><x/>',
        sitemap([], True),
        sitemap(PARTS * 2, True),
        sitemap([f"{BASE}/sitemap.xml?page={i}" for i in range(33)], True),
        f'<sitemapindex xmlns="{crawler.SITEMAP_NS[1:-1]}"><sitemap/></sitemapindex>',
        f'<sitemapindex xmlns="{crawler.SITEMAP_NS[1:-1]}"><unexpected/></sitemapindex>',
        "x" * (crawler.MAX_SITEMAP_BYTES + 1),
    ],
)
def test_invalid_documents_are_not_cached(
    monkeypatch: pytest.MonkeyPatch, document: str
) -> None:
    context = Mock()
    monkeypatch.setattr(zyte_api, "fetch", Mock(return_value=response(document)))
    with pytest.raises((ValueError, etree.XMLSyntaxError)):
        crawler.fetch_sitemap(context, SITEMAP_URL, index=True)
    context.cache.set.assert_not_called()
    context.cache.delete.assert_called_once_with("cache-key")


def test_bad_cached_document_is_invalidated(monkeypatch: pytest.MonkeyPatch) -> None:
    context = Mock()
    monkeypatch.setattr(
        zyte_api, "fetch", Mock(return_value=response("<html/>", cached=True))
    )
    with pytest.raises(ValueError):
        crawler.fetch_sitemap(context, SITEMAP_URL, index=True)
    context.cache.delete.assert_called_once_with("cache-key")


def test_status_and_validated_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    document = sitemap(PARTS, True)
    context = Mock()
    fetch = Mock(
        side_effect=[
            response(document, status=403),
            response(document),
            response(document, cached=True),
        ]
    )
    monkeypatch.setattr(zyte_api, "fetch", fetch)
    with pytest.raises(ValueError, match="HTTP 200"):
        crawler.fetch_sitemap(context, SITEMAP_URL, index=True)
    context.cache.set.assert_not_called()
    assert crawler.fetch_sitemap(context, SITEMAP_URL, index=True) == PARTS
    assert crawler.fetch_sitemap(context, SITEMAP_URL, index=True) == PARTS
    context.cache.set.assert_called_once_with("cache-key", document)


def test_nested_index_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    fetch = Mock(return_value=response(sitemap(PARTS, True)))
    monkeypatch.setattr(zyte_api, "fetch", fetch)
    with pytest.raises(ValueError, match="document type"):
        crawler.discover_profiles(Mock(data_url=SITEMAP_URL))
    assert fetch.call_count == 2
