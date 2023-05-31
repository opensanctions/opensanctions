import string
from typing import Dict, Any, List, Tuple
from requests.exceptions import HTTPError
from normality import collapse_spaces, stringify

from opensanctions.core import Context
from opensanctions import helpers as h

# Useful notes: https://www.fer.xyz/2021/08/interpol

MAX_RESULTS = 160
SEEN = set()
COUNTRIES_URL = "https://www.interpol.int/en/How-we-work/Notices/View-Red-Notices"
FORMATS = ["%Y/%m/%d", "%Y/%m", "%Y"]
GENDERS = ["M", "F", "U"]
AGE_MIN = 0
AGE_MAX = 120


def get_countries(context: Context) -> List[Tuple[str, str]]:
    doc = context.fetch_html(COUNTRIES_URL)
    path = ".//select[@id='arrestWarrantCountryId']//option"
    options = []
    for option in doc.findall(path):
        code = stringify(option.get("value"))
        if code is None:
            continue
        label = collapse_spaces(option.text_content())
        options.append((code, label))
    return list(sorted(options))


def patch(query: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    new = query.copy()
    new.update(update)
    return new


def crawl_notice(context: Context, notice: Dict[str, Any]):
    url = notice.get("_links", {}).get("self", {}).get("href")
    if url in SEEN:
        return
    SEEN.add(url)
    # context.log.info("Crawl notice: %s" % url)
    try:
        notice = context.fetch_json(url, cache_days=7)
    except HTTPError as err:
        context.log.warning(
            "HTTP error",
            url=str(err.request.url),
            error=err.response.status_code,
        )
        return
    first_name = notice["forename"] or ""
    last_name = notice["name"] or ""
    entity = context.make("Person")
    entity.id = context.make_slug(notice.get("entity_id"))
    entity.add("name", first_name + " " + last_name)
    entity.add("firstName", first_name)
    entity.add("lastName", last_name)
    entity.add("sourceUrl", url)
    entity.add("nationality", notice.get("nationalities"))
    entity.add("gender", notice.get("sex_id"))
    entity.add("birthPlace", notice.get("place_of_birth"))

    dob_raw = notice["date_of_birth"]
    entity.add("birthDate", h.parse_date(dob_raw, FORMATS))
    if "v1/red" in url:
        entity.add("topics", "crime")

    for idx, warrant in enumerate(notice.get("arrest_warrants", []), 1):
        # TODO: make this a Sanction:
        entity.add("program", warrant["issuing_country_id"])
        entity.add("notes", warrant["charge"])

    context.emit(entity, target=True)


def crawl_query(
    context: Context,
    query: Dict[str, Any],
):
    context.inspect(query)
    params = query.copy()
    params["resultPerPage"] = MAX_RESULTS
    try:
        data = context.fetch_json(context.source.data.url, params=params, cache_days=5)
    except HTTPError as err:
        context.log.warning(
            "HTTP error",
            url=str(err.request.url),
            error=err.response.status_code,
        )
        return

    notices = data.get("_embedded", {}).get("notices", [])
    for notice in notices:
        crawl_notice(context, notice)
    total = data.get("total")
    if total > MAX_RESULTS:
        context.log.info("More than one page of results", query=query)
        age_max = query.get("ageMax", AGE_MAX)
        age_min = query.get("ageMin", AGE_MIN)
        age_range = age_max - age_min
        # if age_range == 0:
        #     if "name" in query:
        #         context.log.warn(
        #             "Too many results",
        #             query=query,
        #         )
        #     else:
        #         for char in string.ascii_lowercase:
        #             prefix = f"^{char}"
        #             crawl_query(context, patch(query, {"name": prefix}))

        if age_range > 1:
            age_split = age_min + (age_range // 2)
            crawl_query(context, patch(query, {"ageMax": age_max, "ageMin": age_split}))
            crawl_query(context, patch(query, {"ageMax": age_split, "ageMin": age_min}))
        else:
            if "name" in query:
                context.log.warn(
                    "Too many results",
                    query=query,
                )
            else:
                for char in string.ascii_uppercase:
                    prefix = f"^{char}"
                    crawl_query(context, patch(query, {"name": prefix}))
        # if age_range == 1:
        #     crawl_query(context, patch(query, {"ageMax": age_min, "ageMin": age_min}))
        #     crawl_query(context, patch(query, {"ageMax": age_max, "ageMin": age_max}))


def crawl(context: Context):
    context.log.info("Loading interpol API cache...")
    context.cache.preload("https://ws-public.interpol.int/notices/%")
    countries = get_countries(context)
    crawl_query(context, {"sexId": "U"})
    crawl_query(context, {"sexId": "F"})
    for char in string.ascii_uppercase:
        prefix = f"^{char}"
        crawl_query(context, {"name": prefix, "arrestWarrantCountryId": "RU"})
        crawl_query(context, {"forename": prefix, "arrestWarrantCountryId": "RU"})
        crawl_query(context, {"name": prefix, "arrestWarrantCountryId": "SV"})
        crawl_query(context, {"forename": prefix, "arrestWarrantCountryId": "SV"})
    for country, label in countries:
        crawl_query(context, {"arrestWarrantCountryId": country})
        crawl_query(context, {"nationality": country})
