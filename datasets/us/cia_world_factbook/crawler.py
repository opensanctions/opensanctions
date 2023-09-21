from typing import Dict, Any, Optional
from normality import slugify, collapse_spaces
import re
from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import OccupancyStatus

WEB_URL = "https://www.cia.gov/the-world-factbook/countries/%s"
DATA_URL = "https://www.cia.gov/the-world-factbook/page-data/countries/%s/page-data.json"

DATES = ["%d %B %Y"]

REGEX_SKIP_CATEGORY_HTML = re.compile(
    "^<em>note</em>"
    "|<strong>chief of state:</strong> Notification Statement:"
    "|Prime Minister HUN MANET succeeded"
    "|<strong>note 1:</strong>"
)
REGEX_HOLDERS = re.compile((
    "(chief of state|head of government): "
    "(?P<role>("
    "(transitional |Transition |Interim )?President( of the Swiss Confederation| of the Territorial Assembly| of the Government of Spain \(prime minister-equivalent\))?|"
    "(Transition |Caretaker |Interim |Sultan and )?Prime Minister|"
    "Administrator Air Vice-Marshal|"
    "Administrator|"
    "Amir|"
    "Chairperson, Presidential Council,?|"
    "Chairman, Presidential Council,"
    "Chairman of the Council of Ministers|"
    "Chairman of the Presidency|"
    "Chancellor|"
    "Chief Executive|"
    "Chief Minister|"
    "Commissioner|"
    "Co-prince|"
    "Crown Prince and Prime Minister|"
    "Emperor|"
    "Governor|"
    "Grand Duke|"
    "King|"
    "Lord of Mann|"
    "Mayor and Chairman of the Island Council|"
    "Minister of State|"
    "Pope|"
    "Premier|"
    "Prime|"
    "Prime Minister, State Administration Council Chair,|"
    "Prince|"
    "Queen|"
    "Secretary of State for Foreign and Political Affairs|"
    "Sovereign Council Chair and Commander-in-Chief of the Sudanese Armed Forces|"
    "State Affairs Commission President|"
    "Sultan and Prime Minister|"
    "Supreme Leader|"
    "Supreme People's Assembly President|"
    "Taoiseach \(Prime Minister\)|"
    "\(Ulu o Tokelau\)"
    ")) "
    "(?P<holder>[^;]+)"
    "(;? represented by (?P<rep_role>"
    "((Acting |Lieutenant[ -])?Governor([ -]General)?"
    "|Administrator( Sperior)?"
    "|Prefect"
    "|UK High Commissioner to New Zealand and Governor \(nonresident\) of the Pitcairn Islands"
    "|High Commissioner"
    ")) (?P<rep_holder>[^;]+))?"
    ";?(?P<remainder>.*)"
))
REGEX_NAME_DATE = re.compile(
    "(?P<name>[\w.,' -]+)(\(since (?P<date>\d+ \w+ \d+)\))?"
)


# =chief of state: Co-prince Emmanuel MACRON (since 14 May 2017); represented by Patrick STROZDA (since 14 May 2017); and Co-prince Archbishop Joan-Enric VIVES i Sicilia (since 12 May 2003); represented by Josep Maria MAURI (since 20 July 2012)


#remainder: ; the president is both chief of state and head of government; Prime Minister Dinesh GUNAWARDENA (since 22 July 2022)


# "((Executive |First )?Vice President|Vice Chairperson, Presidential Council|Heir Apparent Prince)"
# ; Co-Vice President MUHAMMAD BIN RASHID Al-Maktum (since 5 January 2006); Co-Vice President MANSUR bin Zayid Al-Nuhayyan (since 29 March 2023); Crown Prince KHALID bin Muhammad Al-Nuhayyan, the eldest son of the monarch, born 14 November 1982; note - MUHAMMAD BIN ZAYID Al-Nuhayyan elected president by the Federal Supreme Council following the death of President KHALIFA bin Zayid Al-Nuhayyan on 13 May 2022
# First Deputy Prime Minister Rebecca KADAGA (since 24 June 2021); Second Deputy Prime Minister Moses ALI (since 21 June 2021); note - the president is both chief of state and head of government

# category: chief of state:
# role: King
# holder: CHARLES III (since 8 September 2022)
# remainder: ; represented by Governor General Tofiga Vaevalu FALANI (since 29 August 2021)

# remainder: ; represented by Governor Nigel DAKIN (since 15 July 2019)

# remainder: ; represented by Governor-General of New Zealand Dame Cindy KIRO (since 21 September 2021); New Zealand is represented by Administrator Ross ARDERN (since May 2018)

# remainder: ; Deputy President Paul MASHSATILE (since 7 March 2023); note - the president is both chief of state and head of government; note - Deputy President David MABUZA resigned 1 March 2023

#co-chiefs of state Captain Regent Alessandro SCARANO and Adele TONNINI 
# san marino

# 2023-09-21 12:33:12 [warning  ] Error parsing holder           [us_cia_world_factbook] dataset=us_cia_world_factbook html=chief of state: TUIMALEALI'IFANO Va’aletoa Sualauvi II (since 21 July 2017)

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
    end_date: Optional[str] = None
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(country, name, role)
    person.add("name", name)
    person.add("position", role)
    person.add("sourceUrl", source_url)

    position_topics = ["gov.national", "gov.head"]
    start_date = h.parse_date(start_date, DATES)
    end_date = h.parse_date(end_date, DATES)
    print(end_date)
    position = h.make_position(context, role, country=country, topics=position_topics)
    occupancy = h.make_occupancy(
        context, 
        person, 
        position,
        start_date=start_date[0] if start_date else None,
        end_date=end_date[0] if end_date else None,
        status=OccupancyStatus.CURRENT
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
    
    try:
        res = context.fetch_json(data_url, cache_days=5)
    except Exception as e:
        print(e)
        return

    executive = get(res["result"]["data"]["fields"]["nodes"], "name", "Executive branch")
    if executive is None:
        return
    categories = executive["data"].split("<br><br>")
    for category_html in categories:
        if REGEX_SKIP_CATEGORY_HTML.match(category_html):
            context.log.info("Skipping bad content", truncated_content=category_html[:100])
            continue
        category_els = html.fromstring(category_html)
        label_els = category_els.findall("./strong")
        if len(label_els) != 1:
            
            context.log.warning("Error parsing label", html=category_html)
            continue
        label_text = label_els[0].text_content()
        if label_text in ["chief of state:", "head of government:"]:
            category_text = category_els.text_content()
            match = REGEX_HOLDERS.match(collapse_spaces(category_text))
            if match is None:
                context.log.warning("Error parsing holder.", html=category_text)
                # If it's just a notice, add it to REGEX_SKIP_CATEGORY
                holders = context.lookup("unparsed_holders", category_text)
                if holders:
                    print("overridden")
                    print(holders)
            else:
                if match.group("rep_role"):
                    print(f"rep for {match.group('role')} {match.group('holder')}")
                    role = match.group("rep_role")
                    holder = match.group("rep_holder")
                else:
                    role = match.group('role')
                    holder = match.group('holder')
                print(f"category: {label_text}\nrole: {role}\nholder: {holder}\nremainder: {match.group('remainder')}")
                name_date = REGEX_NAME_DATE.match(holder)
                if name_date:
                    emit_person(context, country, source_url, role, name_date.group("name"), name_date.group("date"))
                else:
                    context.log.warning("Couldn't parse name.", holder=holder)
            print()
    


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=5)
    countries = data["data"]["countries"]["nodes"]
    for c in countries:
        if c["name"] not in SKIP_COUNTRIES:
            crawl_country(context, c["name"])
    # print(REGEX_HOLDERS.pattern)