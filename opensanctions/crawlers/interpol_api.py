import string
from typing import Dict, Any, List, Tuple, Optional, Set
from requests.exceptions import HTTPError
from normality import collapse_spaces, stringify

from opensanctions.core import Context
from opensanctions import helpers as h

# Useful notes: https://www.fer.xyz/2021/08/interpol

IGNORE_FIELDS = {
    "languages_spoken_ids",
    "hairs_id",
    "height",
    "weight",
    "eyes_colors_id",
}
MAX_RESULTS = 160
SEEN_URLS: Set[str] = set()
SEEN_IDS: Set[str] = set()
COUNTRIES_URL = "https://www.interpol.int/en/How-we-work/Notices/View-Red-Notices"
FORMATS = ["%Y/%m/%d", "%Y/%m", "%Y"]
GENDERS = ["M", "F", "U"]
AGE_MIN = 0
AGE_MAX = 120


def get_countries(context: Context) -> List[Any]:
    doc = context.fetch_html(COUNTRIES_URL)
    path = ".//select[@id='arrestWarrantCountryId']//option"
    options: List[Any] = []
    for option in doc.findall(path):
        # code = stringify(option.get("value"))
        # if code is None:
        #     continue
        # label = collapse_spaces(option.text_content())
        options.append(option.get("value"))
    return options


def patch(query: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    new = query.copy()
    new.update(update)
    return new


def crawl_notice(context: Context, notice: Dict[str, Any]):
    _links: Dict[str, Any] = notice.pop("_links", {})
    url: Optional[str] = _links.get("self", {}).get("href")
    if url in SEEN_URLS or url is None:
        return
    SEEN_URLS.add(url)
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
    notice.pop("_links", {})
    notice.pop("_embedded", {})
    entity_id = notice.pop("entity_id")
    if entity_id in SEEN_IDS:
        context.log.warning("Duplicate entity ID", entity_id=entity_id)
    SEEN_IDS.add(entity_id)
    first_name = notice.pop("forename", None)
    last_name = notice.pop("name")
    entity = context.make("Person")
    entity.id = context.make_slug(entity_id)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("sourceUrl", url)
    entity.add("nationality", notice.pop("nationalities", []))
    entity.add("country", notice.pop("country_of_birth_id", []))
    entity.add("gender", notice.pop("sex_id", None))
    entity.add("birthPlace", notice.pop("place_of_birth", None))
    entity.add("notes", notice.pop("distinguishing_marks", None))

    dob_raw = notice.pop("date_of_birth", None)
    entity.add("birthDate", h.parse_date(dob_raw, FORMATS))
    if "v1/red" in url:
        entity.add("topics", "crime")

    for warrant in notice.pop("arrest_warrants", []):
        sanction = h.make_sanction(context, entity)
        sanction.add("program", "Red Notice")
        sanction.add("authorityId", entity_id)
        sanction.add("country", warrant.pop("issuing_country_id", None))
        sanction.add("reason", warrant.pop("charge"))
        sanction.add("reason", warrant.pop("charge_translation"), lang="eng")
        h.audit_data(warrant)
        context.emit(sanction)

    h.audit_data(notice, ignore=IGNORE_FIELDS)
    context.emit(entity, target=True)


def crawl_query(
    context: Context,
    query: Dict[str, Any],
):
    context.inspect(query)
    params = query.copy()
    params["resultPerPage"] = MAX_RESULTS
    try:
        data = context.fetch_json(context.source.data.url, params=params, cache_days=3)
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
    total: int = data.get("total", 0)
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


def crawl(context: Context):
    context.log.info("Loading interpol API cache...")
    context.cache.preload("https://ws-public.interpol.int/notices/%")
    crawl_query(context, {"sexId": "U"})
    crawl_query(context, {"sexId": "F"})
    for char in string.ascii_uppercase:
        prefix = f"^{char}"
        crawl_query(context, {"name": prefix, "arrestWarrantCountryId": "RU"})
        crawl_query(context, {"forename": prefix, "arrestWarrantCountryId": "RU"})
        crawl_query(context, {"name": prefix, "arrestWarrantCountryId": "SV"})
        crawl_query(context, {"forename": prefix, "arrestWarrantCountryId": "SV"})
    for country in get_countries(context):
        crawl_query(context, {"arrestWarrantCountryId": country})
        crawl_query(context, {"nationality": country})

    context.log.info("Seen", ids=len(SEEN_IDS), urls=len(SEEN_URLS))
