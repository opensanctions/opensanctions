import re
from urllib.parse import urlsplit

from lxml import etree
from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

# Profile slugs look like /fugitives/jane-doe. /fugitives/all is the listing.
PROFILE_PATH = re.compile(r"/fugitives/(?!all$)[a-z0-9-]+")


def crawl_sitemap(context: Context, url: str, tag: str) -> list[str]:
    """Fetch a sitemap index or sitemap and return the locations it lists."""
    _, _, _, text = zyte_api.fetch_text(context, url)
    root = etree.fromstring(text.encode("utf-8"))
    h.remove_namespace(root)
    assert root.tag == tag, (url, root.tag)
    return h.xpath_strings(root, "./*/loc/text()")


def crawl_item(fugitive_url: str, context: Context) -> None:
    context.log.info("Fetching fugitive profile via Zyte", url=fugitive_url)
    title_xpath = '//h2[@class="fugitive__title"]'
    # The sitemap can list profiles that were taken down, which render a
    # "Page Not Found" page instead of a profile.
    not_found_xpath = '//h1[@class="page-title"][normalize-space()="Page Not Found"]'
    # Either heading proves we got past the bot protection, so both serve as
    # the unblock validator and as what the browser waits for.
    page_xpath = f"{title_xpath} | {not_found_xpath}"
    # Akamai's interstitial clears itself with a scripted redirect, so browser
    # rendering alone still snapshots the challenge page. Wait for the heading
    # instead.
    response = zyte_api.fetch_html(
        context,
        fugitive_url,
        unblock_validator=page_xpath,
        actions=[
            {
                "action": "waitForSelector",
                "selector": {"type": "xpath", "value": page_xpath},
                "timeout": 15,
            },
        ],
        javascript=True,
        cache_days=7,
        geolocation="US",
    )

    # The sitemap lists some profiles that no longer exist on the site. These
    # are expected, so skip them without a warning.
    if h.xpath_elements(response, not_found_xpath):
        context.log.info("Fugitive profile not found, skipping", url=fugitive_url)
        return

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
    urls: set[str] = set()
    for sitemap_url in crawl_sitemap(context, context.data_url, "sitemapindex"):
        for url in crawl_sitemap(context, sitemap_url, "urlset"):
            if PROFILE_PATH.fullmatch(urlsplit(url).path):
                urls.add(url)
    context.log.info("Discovered fugitive profiles", count=len(urls))
    for url in sorted(urls):
        crawl_item(url, context)
