from zavod import Context

# Don't hit the cache for this - these long jobs stop the db from vacuuming
# if they do.
CACHE_DAYS_DISABLED = None

FORMDATA = {
    "index": "gbd_ul",
    "version": "v1",
    "page": "1",
    "count": "20",
    "text": "",
    "column": "id",
    "order": "ascending",
}

FIELDS_MAPPING = {
    "datereg": "incorporationDate",
    "nameru": "name",
    "addresskz": "address",
    "namekz": "name",
    "bin": "registrationNumber",
    "addressru": "address",
    "statuskz": "status",
    "statusru": "status",
    "okedru": "notes",
    "okedkz": "notes",
}


HEADERS = {
    "Referer": "https://data.egov.kz/datasets/view?index=gbd_ul",
    "X-Requested-With": "XMLHttpRequest",
}


def crawl_page(context: Context, page_number: int) -> int:
    """
    Crawls a single page of the Kazakhstan data portal and emits the data.
    Args:
        context: The context object for the current dataset.
        page_number: The page number to crawl.
    Returns:
        The total number of pages.
    """

    formdata = FORMDATA.copy()
    formdata["page"] = str(page_number)

    data = context.fetch_json(
        context.data_url,
        cache_days=CACHE_DAYS_DISABLED,
        headers=HEADERS,
        params=formdata,
    )

    for item in data["elements"]:
        if item["datereg"]:
            item["datereg"] = item["datereg"][:10]

        entity = context.make("Company")
        if item["bin"]:
            entity.id = context.make_slug("co", item["bin"])
        else:
            entity.id = context.make_id(
                "company", item["id"], item["namekz"] or item["nameru"]
            )

        for k, v in FIELDS_MAPPING.items():
            if k in item and item.get(k):
                lang = None
                if k.endswith("ru"):
                    lang = "rus"
                elif k.endswith("kz"):
                    lang = "kaz"

                entity.add(v, item.get(k), lang=lang)

        entity.add("country", "kz")
        context.emit(entity, target=True)

        if item.get("director"):
            director = context.make("Person")
            director.id = context.make_id("DIRECTOR", entity.id, item["director"])
            director.add("name", item["director"], lang="rus")
            context.emit(director)

            link = context.make("Directorship")
            link.id = context.make_id("Directorship", entity.id, director.id)

            link.add("organization", entity.id)
            link.add("director", director.id)
            link.add("role", "director", lang="eng")
            link.add("role", "директор", lang="rus")

            context.emit(director)
            context.emit(link)

    return data.get("totalPages", 1)


def crawl(context: Context):
    """
    Main function to crawl and process data from the Kazakhstan data portal.
    """

    num_pages = crawl_page(context, 1)

    context.log.info(f"Total pages: {num_pages}")

    for page_number in range(2, num_pages + 1):
        crawl_page(context, page_number)
