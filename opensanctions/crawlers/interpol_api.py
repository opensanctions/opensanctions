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
AGE_MIN = 20
AGE_MAX = 90


def get_countries(context: Context) -> List[Any]:
    doc = context.fetch_html(COUNTRIES_URL, cache_days=7)
    path = ".//select[@id='arrestWarrantCountryId']/option"
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
) -> int:
    # context.inspect(query)
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
        return 0

    total: int = data.get("total", 0)
    notices = data.get("_embedded", {}).get("notices", [])
    for notice in notices:
        crawl_notice(context, notice)

    return total


def crawl(context: Context):
    context.log.info("Loading interpol API cache...")
    # context.cache.preload("https://ws-public.interpol.int/notices/%")
    # crawl_query(context, {"sexId": "U"})
    countries = get_countries(context)
    covered_countries = set()

    for country in countries:
        query = {"nationality": country}
        crawl_query(context, query)
        query = {"arrestWarrantCountryId": country}
        warrant_total = crawl_query(context, query)
        if warrant_total <= MAX_RESULTS:
            covered_countries.add(country)
        else:
            context.log.info("Country has many warrants", country=country)

    for gender in GENDERS:
        crawl_query(context, {"sexId": gender})

    age_query = patch(query, {"ageMax": AGE_MIN, "ageMin": 0})
    if crawl_query(context, age_query) > MAX_RESULTS:
        context.log.warn("Adjust min age", query=age_query)

    age_query = patch(query, {"ageMax": 300, "ageMin": AGE_MAX})
    if crawl_query(context, age_query) > MAX_RESULTS:
        context.log.warn("Adjust max age", query=age_query)

    for dots in range(0, 70):
        pattern = f"^{'.' * dots}$"
        for field in ("name", "forename"):
            query = {field: pattern}
            if crawl_query(context, query) > MAX_RESULTS:
                for age in range(AGE_MIN, AGE_MAX + 1):
                    age_query = patch(query, {"ageMax": age, "ageMin": age})
                    if crawl_query(context, age_query) > MAX_RESULTS:
                        context.log.warn("XXX", query=age_query)
                for country in countries:
                    if country in covered_countries:
                        continue
                    country_query = patch(query, {"arrestWarrantCountryId": country})
                    if crawl_query(context, country_query) > MAX_RESULTS:
                        if field == "forename":
                            continue
                        for odots in range(0, 50):
                            other = f"^{'.' * odots}$"
                            full_query = patch(country_query, {"forename": other})
                            if crawl_query(context, full_query) > MAX_RESULTS:
                                context.log.warn(
                                    "Too many results in full query",
                                    query=full_query,
                                )

    context.log.info(
        "Seen",
        ids=len(SEEN_IDS),
        urls=len(SEEN_URLS),
    )
