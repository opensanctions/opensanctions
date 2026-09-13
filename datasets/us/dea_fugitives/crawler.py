import re
from urllib.parse import urlsplit

from lxml import etree
from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

# The HTML listing can fail even with browser rendering. DEA publishes the
# profile URLs in its XML sitemap; both sitemaps and profiles work through
# Zyte's cheaper HTTP mode. Do not infer an empty list from an HTML challenge.
SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
MAX_SITEMAPS = 32
MAX_SITEMAP_BYTES = 2 * 1024 * 1024
MAX_SITEMAP_URLS = 10_000
PROFILE_PATH = re.compile(r"/fugitives/[a-z0-9_-]+")


def fetch_sitemap(context: Context, url: str, index: bool) -> list[str]:
    result = zyte_api.fetch(
        context,
        zyte_api.ZyteAPIRequest(url=url),
        cache_days=1,
    )
    try:
        if not result.from_cache and result.status_code != 200:
            raise ValueError("DEA sitemap did not return HTTP 200")
        content = result.response_text.encode("utf-8")
        if len(content) > MAX_SITEMAP_BYTES:
            raise ValueError("DEA sitemap exceeds the size limit")
        parser = etree.XMLParser(
            resolve_entities=False, load_dtd=False, no_network=True
        )
        root = etree.fromstring(content, parser=parser)
        if root.getroottree().docinfo.doctype:
            raise ValueError("DEA sitemap must not contain a DTD")
        tag, entry, limit = (
            ("sitemapindex", "sitemap", MAX_SITEMAPS)
            if index
            else ("urlset", "url", MAX_SITEMAP_URLS)
        )
        if root.tag != SITEMAP_NS + tag:
            raise ValueError("Unexpected DEA sitemap document type")
        if any(
            isinstance(child.tag, str) and child.tag != SITEMAP_NS + entry
            for child in root
        ):
            raise ValueError("Unexpected DEA sitemap entry type")
        entries = root.findall(SITEMAP_NS + entry)
        if not 0 < len(entries) <= limit:
            raise ValueError("DEA sitemap entry count is outside its bounds")
        urls = []
        for item in entries:
            locations = item.findall(SITEMAP_NS + "loc")
            if len(locations) != 1 or not locations[0].text:
                raise ValueError("DEA sitemap entry must have one location")
            location = locations[0].text.strip()
            parsed = urlsplit(location)
            if (
                parsed.scheme != "https"
                or parsed.netloc != "www.dea.gov"
                or parsed.fragment
                or any(ord(char) <= 32 or ord(char) == 127 for char in location)
            ):
                raise ValueError("DEA sitemap location is not a same-origin HTTPS URL")
            urls.append(location)
        if index and len(set(urls)) != len(urls):
            raise ValueError("DEA sitemap index repeats a partition")
    except (ValueError, etree.XMLSyntaxError):
        result.invalidate_cache(context)
        raise
    if not result.from_cache:
        context.cache.set(result.cache_fingerprint, result.response_text)
    return urls


def discover_profiles(context: Context) -> list[str]:
    profiles: set[str] = set()
    for sitemap in fetch_sitemap(context, context.data_url, index=True):
        for url in fetch_sitemap(context, sitemap, index=False):
            parsed = urlsplit(url)
            if not parsed.path.startswith("/fugitives/"):
                continue
            if parsed.path == "/fugitives/all":
                continue
            if not PROFILE_PATH.fullmatch(parsed.path) or parsed.query:
                raise ValueError("Unexpected DEA fugitive profile URL")
            profiles.add(url)
    # Bound discovery before fetching any profiles; the dataset's export
    # assertions still validate the resulting Person entities independently.
    if not 400 <= len(profiles) <= 1200:
        raise ValueError("DEA sitemap profile count is outside the 400–1,200 bounds")
    context.log.info("Discovered DEA profiles from sitemap", count=len(profiles))
    return sorted(profiles)


def crawl_item(fugitive_url: str, context: Context) -> None:
    response = zyte_api.fetch_html(
        context,
        fugitive_url,
        unblock_validator='//h2[@class="fugitive__title"]',
        html_source="httpResponseBody",
        cache_days=7,
    )

    name = response.findtext('.//h2[@class="fugitive__title"]')
    table = response.find(".//table")
    assert table is not None, "No table found on fugitive page"
    info_dict = {
        h.element_text(row["label"]): h.element_text(row["description"])
        for row in h.parse_html_table(table)
    }

    entity = context.make("Person")
    entity.id = context.make_id(fugitive_url)
    entity.add("name", name)
    entity.add("sourceUrl", fugitive_url)
    entity.add("topics", "crime")
    entity.add("topics", "wanted")
    entity.add("gender", info_dict.pop("Sex", None))
    entity.add("birthDate", info_dict.pop("Year of Birth", None))
    entity.add("address", info_dict.pop("Last Known Address", None))
    entity.add("ethnicity", info_dict.pop("Race", None))
    entity.add("height", info_dict.pop("Height", None))
    entity.add("weight", info_dict.pop("Weight", None))
    entity.add("hairColor", info_dict.pop("Hair Color", None))
    entity.add("eyeColor", info_dict.pop("Eye Color", None))
    entity.add("notes", info_dict.pop("Notes", None))

    for meta in response.findall('.//div[@class="meta"]'):
        heading = meta.findtext("./*[@class='meta__heading']")
        if heading is None:
            context.log.warning("No heading found in meta, skipping", url=fugitive_url)
            continue
        text = meta.findtext("./*[@class='meta__value']")
        if text is None:
            context.log.warning("No text found in meta, skipping", url=fugitive_url)
            continue

        if "Wanted for the following" in heading:
            entity.add("notes", f"{heading} {text}")

        if "AKA" in heading:
            aliases = h.multi_split(text, [" and ", ";", "/", ","])
            aliases = [a for a in aliases if a.strip() not in ["None", "N/A", "-"]]
            # Sometimes aliases are quoted
            aliases = [a.strip('"') for a in aliases]

            entity.add("weakAlias", aliases)

    for key, val in info_dict.items():
        entity.add("notes", f"{key}: {val}")

    context.emit(entity)
    # context.audit_data(info_dict)


def crawl(context: Context) -> None:
    # Complete discovery first: a failed partition must never yield a partial crawl.
    for url in discover_profiles(context):
        crawl_item(url, context)
