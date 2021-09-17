from normality import collapse_spaces, stringify
from lxml import html
from prefixdate import parse_formats

from opensanctions import settings
from opensanctions.helpers import clean_gender

MAX_RESULTS = 160
SEEN = set()
COUNTRIES_URL = "https://www.interpol.int/en/How-we-work/Notices/View-Red-Notices"
FORMATS = ["%Y/%m/%d", "%Y/%m", "%Y"]


def get_countries(context):
    res = context.http.get(COUNTRIES_URL)
    doc = html.fromstring(res.text)
    path = ".//select[@id='arrestWarrantCountryId']//option"
    options = []
    for option in doc.findall(path):
        code = stringify(option.get("value"))
        if code is None:
            continue
        label = collapse_spaces(option.text_content())
        options.append((code, label))
    return list(sorted(options))


def crawl_notice(context, notice):
    url = notice.get("_links", {}).get("self", {}).get("href")
    if url in SEEN:
        return
    SEEN.add(url)
    res = context.http.get(url)
    if not res.ok:
        context.log.warning("HTTP error", url=res.url, error=res.status_code)
        return
    # if not res.from_cache:
    #     time.sleep(0.5)
    notice = res.json()
    first_name = notice["forename"] or ""
    last_name = notice["name"] or ""
    entity = context.make("Person")
    entity.make_slug(notice.get("entity_id"))
    entity.add("name", first_name + " " + last_name)
    entity.add("firstName", first_name)
    entity.add("lastName", last_name)
    entity.add("sourceUrl", url)
    entity.add("nationality", notice.get("nationalities"))
    entity.add("gender", clean_gender(notice.get("sex_id")))
    entity.add("birthPlace", notice.get("place_of_birth"))

    dob = parse_formats(notice["date_of_birth"], FORMATS)
    entity.add("birthDate", dob)
    # entity.add("keywords", "REDNOTICE")
    # entity.add("topics", "crime")

    for idx, warrant in enumerate(notice.get("arrest_warrants", []), 1):
        # TODO: make this a Sanction:
        entity.add("program", warrant["issuing_country_id"])
        entity.add("notes", warrant["charge"])

    context.emit(entity, target=True, unique=True)


def crawl_country(context, country, age_max=120, age_min=0):
    params = {
        "ageMin": int(age_min),
        "ageMax": int(age_max),
        # "arrestWarrantCountryId": country,
        "nationality": country,
        "resultPerPage": MAX_RESULTS,
        "_": settings.RUN_DATE,
    }
    res = context.http.get(context.dataset.data.url, params=params)
    if res.status_code != 200:
        context.log.warning(
            "HTTP error", url=res.url, country=country, error=res.status_code
        )
        return
    # if not res.from_cache:
    #     time.sleep(0.5)
    data = res.json()
    notices = data.get("_embedded", {}).get("notices", [])
    for notice in notices:
        crawl_notice(context, notice)
    total = data.get("total")
    # pprint((country, total, age_max, age_min))
    if total > MAX_RESULTS:
        age_range = age_max - age_min
        if age_range > 1:
            age_split = age_min + (age_range // 2)
            crawl_country(context, country, age_max, age_split)
            crawl_country(context, country, age_split, age_min)
        elif age_range == 1:
            crawl_country(context, country, age_max, age_max)
            crawl_country(context, country, age_min, age_min)


def crawl(context):
    for country, label in get_countries(context):
        context.log.info("Crawl %r" % label, code=country)
        crawl_country(context, country)
