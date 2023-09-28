from typing import Dict, Any, Optional
from normality import slugify, collapse_spaces
import re
from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import OccupancyStatus

WEB_URL = "https://www.cia.gov/the-world-factbook/countries/%s"
DATA_URL = (
    "https://www.cia.gov/the-world-factbook/page-data/countries/%s/page-data.json"
)

DATES = ["%d %B %Y"]

REGEX_SKIP_CATEGORY_HTML = re.compile(
    "("
    "<em>note</em>"
    "|<strong>chief of state:</strong> Notification Statement:"
    "|Prime Minister HUN MANET succeeded"
    "|<strong>note 1:</strong>"
    ")"
)
REGEX_RELEVANT_CATEGORY = re.compile("^(chief of state|head of government): ")
REGEX_HOLDER = re.compile(
    (
        "(represented by )?"
        "(?P<role>("
        "(transitional |Transition |Interim )?President"
        "|President( of the Swiss Confederation| of the Territorial Assembly| of the Government of Spain \(prime minister-equivalent\))?"
        "|(First |Second |Executive |Co-)?(Vice |Deputy )President"
        "|(Acting |Transition |Caretaker |Interim |Sultan and |(First |Second |Third )?Deputy )?Prime Minister"
        "|Administrator Air Vice-Marshal"
        "|Administrator"
        "|Amir"
        "|Bailiff"
        "|(Vice )?Chairperson, Presidential Council,?"
        "|Chairman, Presidential Council,"
        "|Chairman of the Council of Ministers"
        "|Chairman of the Presidency"
        "|Chancellor"
        "|Chief Executive"
        "|Chief Minister"
        "|(New Zealand )?(High )?Commissioner"
        "|Co-prince"
        "|Crown Prince and Prime Minister"
        "|Crown Prince"
        "|Emperor"
        "|(Vice |Acting |Lieutenant[ -])?Governor([ -]General)?( of the Commonwealth of Australia)?"
        "|Grand Duke"
        "|King"
        "|Lord of Mann"
        "|Mayor and Chairman of the Island Council"
        "|Minister of State"
        "|Pope"
        "|Prefect"
        "|(First Deputy |Vice )?Premier"
        "|Prime"
        "|Prime Minister, State Administration Council Chair,"
        "|Prince"
        "|Queen"
        "|Secretary of State for Foreign and Political Affairs"
        "|Sovereign Council Chair and Commander-in-Chief of the Sudanese Armed Forces"
        "|State Affairs Commission President"
        "|Sultan and Prime Minister"
        "|Supreme Leader"
        "|Supreme People's Assembly President"
        "|Taoiseach \(Prime Minister\)"
        "|\(Ulu o Tokelau\)"
        ")) "
        "(?P<name>[\w,.'’\" -]+) ?"
        "(\([\w \.]+\))? ?"
        "\((since |born |reappointed )?(?P<start_date>\d* ?\w* ?\d{4} ?)\)"
    )
)

SKIP_COUNTRIES = {
    "World",
    "European Union",
}


def emit_person(
    context: Context,
    country: str,
    source_url: str,
    role: str,
    name: str,
    start_date: Optional[str],
    end_date: Optional[str] = None,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(country, name, role)
    person.add("name", name)
    person.add("position", role)
    person.add("sourceUrl", source_url)

    position_topics = ["gov.national", "gov.head"]
    start_date = h.parse_date(start_date, DATES, None)
    end_date = h.parse_date(end_date, DATES, None)
    position = h.make_position(
        context,
        role,
        country=country,
        topics=position_topics,
        id_hash_prefix="us_cia_world_leaders",
    )
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date[0] if start_date else None,
        end_date=end_date[0] if end_date else None,
        status=OccupancyStatus.CURRENT,
    )

    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)


def get(items, key, value):
    for item in items:
        if item[key] == value:
            return item
    return None


def crawl_country(context: Context, country: str) -> None:
    context.log.info("Crawling country: %s" % country)
    country_slug = slugify(country.replace("'", ""), sep="-")
    data_url = DATA_URL % country_slug
    source_url = WEB_URL % country_slug
    response = context.fetch_json(data_url, cache_days=5)

    executive = get(
        response["result"]["data"]["fields"]["nodes"], "name", "Executive branch"
    )
    if executive is None:
        return
    categories = executive["data"].split("<br><br>")
    for category_html in categories:
        if REGEX_SKIP_CATEGORY_HTML.match(category_html):
            context.log.info(
                "Skipping bad content", truncated_content=category_html[:100]
            )
            continue
        category_els = html.fromstring(category_html)
        label_els = category_els.findall("./strong")
        if len(label_els) != 1:
            context.log.warning("Error parsing label", html=category_html)
            continue
        label_text = label_els[0].text_content()
        if label_text in ["chief of state:", "head of government:"]:
            category_text = category_els.text_content()
            category_text = REGEX_RELEVANT_CATEGORY.sub("", category_text)
            for segment in collapse_spaces(category_text).split("; "):
                match = REGEX_HOLDER.match(segment)
                if match is None:
                    res = context.lookup("unparsed_holders", segment)
                    if res:
                        for holder in res.holders:
                            emit_person(
                                context,
                                country,
                                source_url,
                                holder["role"],
                                holder["name"],
                                holder.get("start_date", None),
                                holder.get("end_date", None),
                            )
                    else:
                        context.log.warning(
                            "Error parsing holder.", html=segment, url=source_url
                        )
                else:
                    role = match.group("role")
                    emit_person(
                        context,
                        country,
                        source_url,
                        role,
                        match.group("name"),
                        match.group("start_date"),
                    )


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=5)
    countries = data["data"]["countries"]["nodes"]
    for c in countries:
        redirect = c["redirect"]
        name = c["name"] if redirect is None else redirect["name"]
        if name not in SKIP_COUNTRIES:
            crawl_country(context, name)
