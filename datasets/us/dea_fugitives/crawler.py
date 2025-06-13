from time import sleep

from zavod import Context

# 1s delay seems to be enough to avoid getting blocked, while it takes a long
# time to get unblocked after about 10 requests.
SLEEP_SECONDS = 1
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.5",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "Priority": "u=1, i",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0 (zavod; opensanctions.org)",
}


def parse_table(table):
    """This function is used to parse the table of Labels and Descriptions
    about the fugitive and return as a dict.
    """
    info_dict = {}
    # The first row will always be the header (Label, Description)
    # So we can skip it.
    for row in table.findall(".//tr")[1:]:
        label = row.findtext(".//td[1]")
        description = row.findtext(".//td[2]")
        info_dict[label] = description
    return info_dict


def crawl_item(fugitive_url: str, context: Context):
    response = context.fetch_html(fugitive_url, cache_days=7, headers=HEADERS)

    name = response.findtext('.//h2[@class="fugitive__title"]')
    info_dict = parse_table(response.find(".//table"))

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
        heading = meta.findtext("./h3")
        if heading is None:
            context.log.warning("No heading found in meta, skipping", url=fugitive_url)
            continue
        text = meta.findtext("./div")
        if text is None:
            context.log.warning("No text found in meta, skipping", url=fugitive_url)
            continue
        if "Wanted for the following" in heading:
            entity.add("notes", text)
        if "AKA" in heading:
            entity.add("weakAlias", text.split(","))

    for key, val in info_dict.items():
        entity.add("notes", f"{key}: {val}")

    context.emit(entity)
    # context.audit_data(info_dict)


def crawl(context: Context):
    # Each page only displays 10 fugitives at a time, so we need to loop until we don't find any more fugitives
    base_url = context.data_url
    page_num = 0

    while True:
        url = base_url + "?page=" + str(page_num)
        context.log.info("Fetching page: %s" % page_num, url=url)
        response = context.fetch_html(url, cache_days=1, headers=HEADERS)
        response.make_links_absolute(url)

        # If there are no more fugitives, we can stop crawling.
        if len(response.findall('.//div[@class="teaser "]/div/h3/a')) == 0:
            break

        for item in response.findall('.//div[@class="teaser "]/div/h3/a'):
            sleep(SLEEP_SECONDS)
            crawl_item(item.get("href"), context)

        page_num += 1
