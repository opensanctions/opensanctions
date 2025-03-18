from collections import defaultdict
from typing import Dict, Any, List, Optional, Set
from requests.exceptions import HTTPError

from zavod import Context
from zavod import helpers as h

# Useful notes: https://www.fer.xyz/2021/08/interpol

CACHE_VSHORT = 1
CACHE_SHORT = 3
CACHE_LONG = 14
IGNORE_FIELDS = [
    "languages_spoken_ids",
    # "hairs_id",
    # "height",
    # "weight",
    # "eyes_colors_id",
]
MAX_RESULTS = 160
SEEN_URLS: Set[str] = set()
SEEN_IDS: Set[str] = set()
COUNTRIES_URL = "https://www.interpol.int/en/notices/data/countries"
GENDERS = ["M", "F", "U"]
AGE_MIN = 20
AGE_MAX = 90
STATUSES = defaultdict(int)
HEADERS = {
    # "accept": "*/*",
    # "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    # "cache-control": "max-age=0",
    # "priority": "u=0, i",
    # "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    # "sec-ch-ua-mobile": '?0',
    # "sec-ch-ua-platform": '"macOS"',
    # "sec-fetch-dest": "document",
    "origin": "https://www.interpol.int",
    "referer": "https://www.interpol.int/",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 (zavod; opensanctions.org)",
}


def get_countries(context: Context) -> List[str]:
    doc = context.fetch_json(COUNTRIES_URL, cache_days=CACHE_VSHORT, headers=HEADERS)
    return [v["value"] for v in doc]


def patch(query: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    new = query.copy()
    new.update(update)
    return new


def crawl_notice(context: Context, notice: Dict[str, Any]) -> None:
    _links: Dict[str, Any] = notice.pop("_links", {})
    url: Optional[str] = _links.get("self", {}).get("href")
    if url in SEEN_URLS or url is None:
        return
    SEEN_URLS.add(url)
    # context.log.info("Crawl notice: %s" % url)
    try:
        notice = context.fetch_json(url, cache_days=CACHE_LONG, headers=HEADERS)
    except HTTPError as err:
        if err.response.status_code == 404:
            return
        context.log.warning(
            "HTTP error",
            url=str(err.request.url),
            error=err.response.status_code,
        )
        STATUSES[err.response.status_code] += 1
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
    entity.add("hairColor", notice.pop("hairs_id", None))
    height = notice.pop("height", None)
    if isinstance(height, float):
        height = "%.2f" % height
    entity.add("height", height)
    weight = notice.pop("weight", None)
    if isinstance(weight, float):
        weight = "%.2f" % weight
    entity.add("weight", weight)
    entity.add("eyeColor", notice.pop("eyes_colors_id", None))

    dob_raw = notice.pop("date_of_birth", None)
    h.apply_date(entity, "birthDate", dob_raw)
    if "v1/red" in url:
        entity.add("topics", "crime")
        entity.add("topics", "wanted")

    for warrant in notice.pop("arrest_warrants", []):
        sanction = h.make_sanction(context, entity)
        sanction.add("program", "Red Notice")
        sanction.add("authorityId", entity_id)
        sanction.add("country", warrant.pop("issuing_country_id", None))
        sanction.add("reason", warrant.pop("charge"))
        sanction.add("reason", warrant.pop("charge_translation"), lang="eng")
        context.audit_data(warrant)
        context.emit(sanction)

    context.audit_data(notice, ignore=IGNORE_FIELDS)
    context.emit(entity)


def crawl_query(context: Context, query: Dict[str, Any]) -> int:
    context.log.info(f"Running query: {query}", query=query)
    params = query.copy()
    params["resultPerPage"] = MAX_RESULTS
    try:
        data = context.fetch_json(
            context.data_url, params=params, cache_days=CACHE_SHORT, headers=HEADERS
        )
    except HTTPError as err:
        if err.response.status_code == 404:
            return 0
        context.log.warning(
            "HTTP error",
            url=str(err.request.url),
            error=err.response.status_code,
        )
        STATUSES[err.response.status_code] += 1
        return 0
    total: int = data.get("total", 0)
    notices = data.get("_embedded", {}).get("notices", [])
    for notice in notices:
        crawl_notice(context, notice)

    return total


def crawl(context: Context) -> None:
    # context.log.info("Loading interpol API cache...")
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

    age_query = {"ageMax": AGE_MIN, "ageMin": 0}
    if crawl_query(context, age_query) > MAX_RESULTS:
        context.log.warn("Adjust min age", query=age_query)

    age_query = {"ageMax": 300, "ageMin": AGE_MAX}
    if crawl_query(context, age_query) > MAX_RESULTS:
        context.log.warn("Adjust max age", query=age_query)

    for dots in range(0, 100):
        pattern = f"^{'.' * dots}$"
        for field in ("name", "forename"):
            query = {field: pattern}
            if crawl_query(context, query) > MAX_RESULTS:
                for age in range(AGE_MIN, AGE_MAX + 1):
                    age_query = patch(query, {"ageMax": age, "ageMin": age})
                    if crawl_query(context, age_query) > MAX_RESULTS:
                        context.log.warn(
                            "Too many names in age bracket",
                            query=age_query,
                        )
                for country in countries:
                    if country in covered_countries:
                        continue
                    country_query = patch(query, {"arrestWarrantCountryId": country})
                    if crawl_query(context, country_query) > MAX_RESULTS:
                        for age in range(AGE_MIN, AGE_MAX):
                            age_query = patch(
                                country_query,
                                {"ageMax": age, "ageMin": age},
                            )
                            age_total = crawl_query(context, age_query)
                            if age_total > MAX_RESULTS:
                                context.log.warn(
                                    "Too many results in full query",
                                    query=age_query,
                                )

    context.log.info(
        "Seen",
        ids=len(SEEN_IDS),
        urls=len(SEEN_URLS),
    )

    if any([key > 200 for key in STATUSES.keys()]):
        raise RuntimeError("non-200 HTTP statuse codes %r" % STATUSES)
