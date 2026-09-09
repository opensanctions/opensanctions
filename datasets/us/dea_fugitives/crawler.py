from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

# The listing pages are served behind an Akamai Bot Manager interstitial
# challenge (HTTP 200 with a `bm-verify` redirect page and no content), so they
# must be rendered in a browser. The individual fugitive pages are not
# challenged when proxied, so the cheaper httpResponseBody scrape is used.


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
    # Each page only displays 10 fugitives at a time, so we need to loop until we don't find any more fugitives
    base_url = context.data_url
    page_num = 0

    while True:
        url = base_url + "?page=" + str(page_num)
        context.log.info(f"Fetching page: {page_num}", url=url)
        response = zyte_api.fetch_html(
            context,
            url,
            unblock_validator='//h1[@class="page-title"]',
            cache_days=1,
            absolute_links=True,
        )

        items = response.findall('.//h3[@class="teaser__heading"]/a')
        # An empty first page means the listing changed or we're blocked,
        # never a legitimately empty list.
        assert page_num > 0 or len(items) > 0, "No fugitives found on first page"
        # If there are no more fugitives, we can stop crawling.
        if len(items) == 0:
            break

        for item in items:
            item_url = item.get("href")
            assert item_url is not None, "No href found on fugitive link"
            crawl_item(item_url, context)

        page_num += 1
