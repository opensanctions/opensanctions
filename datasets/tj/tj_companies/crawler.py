from time import sleep

from zavod import Context
from zavod import helpers as h
from rigour.urls import build_url


CACHE_DAYS = 7
SLEEP_TIME = 1

def get_callback_params(page):
    if page > 9999:
        return f"c0:KV|2;[];GB|24;12|PAGERONCLICK7|PN{page};"
    if page > 999:
        return f"c0:KV|2;[];GB|23;12|PAGERONCLICK6|PN{page};"
    elif page > 99:
        return f"c0:KV|2;[];GB|22;12|PAGERONCLICK5|PN{page};"
    elif page > 9:
        return f"c0:KV|2;[];GB|21;12|PAGERONCLICK4|PN{page};"

    return f"c0:KV|2;[];GB|20;12|PAGERONCLICK3|PN{page};"


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
        CRAWL_URL,
        cache_days=CACHE_DAYS,
        headers=HEADERS,
        params=formdata,
    )

    for item in data["elements"]:
        if item["datereg"]:
            plus_idx = item["datereg"].find("+")
            item["datereg"] = h.parse_date(
                item["datereg"][0:plus_idx] if plus_idx != -1 else item["datereg"],
                ["%Y-%m-%d"],
            )[0]

        entity = context.make("Company")
        entity.id = context.make_id("KZCompany", item["id"], item["bin"])

        for k, v in FIELDS_MAPPING.items():
            if k in item and item.get(k):
                lang = None
                if k.endswith("ru"):
                    lang = "rus"
                elif k.endswith("kz"):
                    lang = "kaz"

                entity.add(v, item.get(k), lang=lang)

        entity.add("country", "kz")
        entity.add("sourceUrl", build_url(CRAWL_URL, params=formdata))

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
        sleep(SLEEP_TIME)
        crawl_page(context, page_number)
