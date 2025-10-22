from typing import Dict, List, Tuple
from urllib.parse import urljoin
import re
import datetime

import requests

from zavod import Context

# It looks like a colon in the company name might break their site, but
# AL YAHMADI za razvoj i projekte Društvo sa ograničenom odgovornošću Sarajevo
# Na engleskom jeziku: AL YAHMADI for development and projects Limited Liability Company Sarajevo
# https://bizreg.pravosudje.ba/pls/apex/f?p=186:13:3129288602127392::NO:RP,13:P13_P_POS_ID,P13_XMBS,P13_NAZIV:11563166%2C65-01-0620-17%2C%5CAL%20YAHMADI%20za%20razvoj%20i%20projekte%20Dru%C5%A1tvo%20sa%20ograni%C4%8Denom%20odgovorno%C5%A1%C4%87u%20Sarajevo%0D%0ANa%20engleskom%20jeziku:%20AL%20YAHMADI%20for%20development%20and%20projects%20Limited%20Liability%20Company%20Sarajevo\&cs=3819C1F7C88FC875F599B4E719587FBBE
# shows an error page, while
# PROJEKT ŽIVOTA I KOMUNIKACIJA d.o.o. Mostar, firma na njemačkom jeziku: Lebensart & kommunikation d.o.o. Mostar
# https://bizreg.pravosudje.ba/pls/apex/f?p=186:13:3129288602127392::NO:RP,13:P13_P_POS_ID,P13_XMBS,P13_NAZIV:21131241%2C58-01-0068-09%2C%5CPROJEKT%20%C5%BDIVOTA%20I%20KOMUNIKACIJA%20d.o.o.%20Mostar%2C%20firma%20na%20njema%C4%8Dkom%20jeziku:%20Lebensart%20&%20kommunikation%20d.o.o.%20Mostar\&cs=372A07D95AE8C2719A055669223E4025E
# responds 400
#
# Other companies with Na engleskom jeziku: don't error.
EXPECTED_ERRORS = 10

# Unfortunately no cache for the listing page, as the state of the current
# page is stored in the session and no cache for details page, as
# the url is always changing
CACHE_DAYS = None

BASE_URL = "https://bizreg.pravosudje.ba/pls/apex/"
# URL to retrieve the list of cities
DICTS_URL = f"{BASE_URL}/wwv_flow.show"
# URL to issue two preliminary requests to get the system recognize us
TOUCH_URL = DICTS_URL
# URL to retrieve the list of companies
RETRIEVE_URL = f"{BASE_URL}/f"

FOUNDER_DENY_LIST = {
    "-",
    "Dioničari prema evidenciji Registra vrijednosnih papir F BiH",
    "Dioničari prema knjizi Dioničara",
    "dioničari prema listi u prilogu",
    "prema listi  dioničari",
    "prema listi dioničari",
}

# MBS	12-34-5678-90	2 digits "-" 2 digits "-" 4 digits "-" 2 digits
# MBS	1-2345-67	1 digit "-" 4 digits "-" 2 digits
# MBS	1-123	1 digit "-" 3 digits
# MBS	1-1234	1 digit "-" 4 digits
# MBS	1-12345	1 digit "-" 5 digits
# JIB	1234567890123	13 digits
# PIB	123456789012	12 digits
REGEX_ROUGH_REGNO = re.compile(r"^\d+-?\d{2,}-?[\d-]+$")
REMOVE_PATTERNS = [
    r"BRISAN(?: USLJED PRIPAJANJA)?",
    r"\(BRISAN USLJED PRIPAJANJA\)",
    r"BRISAN iz sudskog registra[-:]?",
    r"\(BRISANO IZ SUDSKOG REGISTRA\)",
    r"BRISAN:.*",
    r"- BRISAN .*",
    r"BRISAN(?: -)?",
    r"BRISAN ZBOG ZAKLJUČENJA LIKVIDACIJE:",
    r"\(u LIKVIDACIJI\)",
    r"U STEČAJU",
    r"LOCAL COM[M]?UNITY",
    r"\(PRESTANAK\),? u likvidaciji",
]
SPLIT_PATTERNS = [
    r"\(skraćeni naziv:.*\)",
    r"skraćena oznaka firme:",
    r"skraćeno:",
]
REMOVE_REGEX = re.compile("|".join(REMOVE_PATTERNS), flags=re.IGNORECASE)
SPLIT_REGEX = re.compile("|".join(SPLIT_PATTERNS), flags=re.IGNORECASE)


def roughly_valid_regno(regno: str) -> bool:
    """
    Check if the registration number is valid
    Args:
        regno: The registration number to check.
    Returns:
        True if the registration number is valid, False otherwise.
    """
    return bool(REGEX_ROUGH_REGNO.match(regno))


def get_secret_param(context: Context) -> str:
    """
    Goes through the chain of redirects to get the secret param.
    Args:
        context: The context object for the current dataset.
    Returns:
        The secret param as a str, or an empty string if not found.
    """
    try:
        resp = context.fetch_text(context.data_url, cache_days=CACHE_DAYS)
        matches = re.search(r"f\?p=18\d\:\d+\:(\d+)", resp)
        if not matches:
            context.log.warning("Cannot find secret param")
            return ""
        return matches.group(1)

    except Exception as e:
        context.log.warning(f"Failed to get secret param: {e}")
        return ""


def clean_name(name: str) -> Tuple[str, str]:
    """
    Clean a single company name string, returning (main_name, alias).
    Alias is empty string if no split is found.
    """
    # Remove unwanted patterns
    cleaned = REMOVE_REGEX.sub("", name).strip(" -:()")
    # Try to extract alias
    match = SPLIT_REGEX.search(cleaned)
    if match:
        main_name = SPLIT_REGEX.sub("", cleaned).strip(" -:(),")
        alias = match.group(1).strip() if match.groups() else ""
        return main_name, alias
    return cleaned, ""  # no alias found


def seed_city(context: Context, secret_param: str) -> List[Dict[str, str]]:
    """
    Fetches the list of cities from the website.
    Args:
        context: The context object for the current dataset.
        secret_param: The secret param for the request.
    Returns:
        The list of cities as a list of dicts.
    """
    payload = {
        "p_request": "APPLICATION_PROCESS=populateShuttleOps",
        "p_instance": secret_param,
        "p_flow_id": "186",
        "p_flow_step_id": "0",
        "x01": "",
        "x02": "",
        "x03": "-1",
        "x04": "-1",
    }
    resp = context.fetch_text(
        url=DICTS_URL,
        method="POST",
        data=payload,
        cache_days=CACHE_DAYS,
    )
    cities = re.findall(r'id: (\d+), data: "([\w /-]+)"', resp)

    return [{"city": city, "code": code} for code, city in cities]


def parse_city(
    context: Context,
    secret_param: str,
    city: Dict[str, str],
    from_date: str,
    to_date: str,
) -> None:
    """
    Fetches the list of companies from the website.
    Args:
        context: The context object for the current dataset.
        secret_param: The secret param for the request.
        city: The city to fetch the companies for.
        from_date: from company registration date.
        to_date: to company registration date.
    """

    TOUCH_PAYLOAD1 = {
        "p_request": "APPLICATION_PROCESS=NAPREDNA_PRETRAGA_PARAMS",
        "p_instance": secret_param,
        "p_flow_id": "186",
        "p_flow_step_id": "0",
        "x01": city["code"],
        "x02": "-1",
        "x03": "-1",
        "x04": "",
        "x05": from_date,
        "x06": to_date,
        "x07": "-1",
        "x08": "-1",
        "x09": "",
    }

    TOUCH_PAYLOAD2 = {
        "p_request": "APPLICATION_PROCESS=NAPREDNA_PRETRAGA_PARAMS_2",
        "p_instance": secret_param,
        "p_flow_id": "186",
        "p_flow_step_id": "0",
        "x01": "-1",
        "x02": "-1",
        "x03": "-1",
    }

    RETRIEVE_PAYLOAD = {
        "p": f"186:3:{secret_param}:FLOW_PPR_OUTPUT_R16339113485096783_pg_"
        + "R_16339113485096783:NO",
        "pg_max_rows": "5000",
        "pg_min_row": "1",
        "pg_rows_fetched": "undefined",
    }

    context.fetch_text(
        url=TOUCH_URL,
        method="POST",
        data=TOUCH_PAYLOAD1,
        cache_days=CACHE_DAYS,
    )

    context.fetch_text(
        url=TOUCH_URL,
        method="POST",
        data=TOUCH_PAYLOAD2,
        cache_days=CACHE_DAYS,
    )

    result = context.fetch_html(
        url=RETRIEVE_URL,
        method="POST",
        data=RETRIEVE_PAYLOAD,
        cache_days=CACHE_DAYS,
    )

    rows = result.findall(".//tr")

    records = []
    for row in rows:
        if row.find(".//td/a") is None:
            continue
        record = {}
        record["city"] = city.get("city", "")
        record["registration_number"] = row[0].text
        record["name"] = row[1][0].text
        record["abbreviation"] = row[2][0].text
        record["address"] = row[3].text
        record["date_of_last_decision"] = row[4].text
        record["details_url"] = urljoin(BASE_URL, row[1][0].attrib["href"])

        records.append(record)

    return records


def crawl_details(context: Context, record: Dict[str, str]) -> None:
    """
    Fetches and emits the details of a company from the website.
    Args:
        context: The context object for the current dataset.
        record: The record to fetch the details for.
    """
    try:
        details_page = context.fetch_html(record["details_url"])
    except requests.exceptions.HTTPError as exc:
        context.log.warning(
            f"Failed to fetch company {record["details_url"]}: {type(exc)}, {exc}"
        )
        return False

    legal_form = details_page.xpath(
        "//td[contains(text(),"
        + ' "Legal form of organization")'
        + "]/following-sibling::td/text()"
    )
    if legal_form:
        record["legal_form"] = legal_form[0]

    address_add = details_page.xpath(
        "//td[contains(text()," + ' "Address")' + "]/following-sibling::td/text()"
    )
    if address_add:
        record["address_additional"] = address_add[0]

    status = details_page.xpath(
        "//td[contains(text(),"
        + ' "Status (Bankruptcy – YES/NO)")'
        + "]/following-sibling::td/text()"
    )
    if status:
        record["status_bankruptcy"] = status[0]

    uin = details_page.xpath(
        "//td[contains(text(),"
        + ' "Unique Identification Number")'
        + "]/following-sibling::td/text()"
    )
    # Jedinstveni identifikacioni broj - JIB or UIN
    # https://www.vatify.eu/bosnia-and-herzegovina-vat-number.html
    if uin:
        record["unique_id"] = uin[0]

    customs_number = details_page.xpath(
        "//td[contains(text(),"
        + ' "Customs Number")'
        + "]/following-sibling::td/text()"
    )

    if customs_number and customs_number[0].replace("\xa0", " ").strip():
        record["customs_number"] = f"Customs number: {customs_number[0]}"

    founders_people = []
    founders_companies = []
    managers = []

    try:
        founders_url = urljoin(
            BASE_URL, details_page.xpath('//*[@id="podmeni"]/p/a')[0].attrib["href"]
        )
    except IndexError:
        context.log.warning("Details page empty", url=record["details_url"])
    else:
        founders_page = context.fetch_html(founders_url)

        names = founders_page.xpath(
            "//td[contains(text(),"
            + ' "Ime osnivača")'
            + "]/following-sibling::td/text()"
        )

        cap_paid = founders_page.xpath(
            "//td[contains(text(),"
            + ' "Kapital [uplaćeni]")'
            + "]/following-sibling::td/text()"
        )

        shares = founders_page.xpath(
            "//td[contains(text(),"
            + ' "Dionice [broj]")'
            + "]/following-sibling::td/text()"
        )

        basic_data = founders_page.xpath(
            "//td[contains(text(),"
            + ' "Basic data")'
            + "]/following-sibling::td/text()"
        )

        reg_num = founders_page.xpath(
            "//td[contains(text(),"
            + ' "Registration Number")'
            + "]/following-sibling::td/text()"
        )

        for i, name in enumerate(names):
            founders_people.append(
                {
                    "name": name,
                    "capital_paid": cap_paid[i],
                    "shares": shares[i],
                }
            )

        for i, bd in enumerate(basic_data):
            parsed_bd = list(map(str.strip, bd.split(" ,")))
            company_name = parsed_bd[0]

            # includes country
            address = ", ".join(parsed_bd[1:]).strip().strip(",")
            country = ""
            if len(parsed_bd) > 1:
                country = parsed_bd[-1]

            founders_companies.append(
                {
                    "basic_data": bd,
                    "registration_number": (
                        reg_num[i] if re.search(r"\d", reg_num[i]) else ""
                    ),
                    "name": company_name,
                    "address": address,
                    "country": country,
                }
            )

        managers_url = urljoin(
            BASE_URL, founders_page.xpath('//*[@id="podmeni"]/p/a')[1].attrib["href"]
        )
        managers_page = context.fetch_html(managers_url)
        managers_names = managers_page.xpath(
            "//td[contains(text()," + ' "Name")]/' + "following-sibling::td/text()"
        )

        managers_pos = managers_page.xpath(
            "//td[contains(text(),"
            + ' "Position")'
            + "]/following-sibling"
            + "::td/text()"
        )[1::2]

        managers_auth = managers_page.xpath(
            "//td[contains(text(),"
            + ' "Authorisations/'
            + ' Position limits")'
            + "]/following-sibling::td/"
            + "text()"
        )
        if managers_names:
            for i, manager in enumerate(managers_names):
                managers.append(
                    {
                        "name": manager,
                        "authorizations": managers_auth[i],
                        "position": managers_pos[i],
                    }
                )

    finally:
        entity = context.make("Company")
        if roughly_valid_regno(record.get("registration_number")):
            entity.id = context.make_slug(record["registration_number"])
        else:
            assert record["name"]
            entity.id = context.make_id("BACompany", record["name"])

        entity.add("name", clean_name(record["name"]), lang="bos")
        entity.add("name", clean_name(record["abbreviation"]), lang="bos")
        entity.add("status", record.get("status_bankruptcy", None), lang="bos")

        entity.add("country", "ba")
        entity.add("address", record["address"], lang="bos")
        entity.add("address", record.get("address_additional", None), lang="bos")

        entity.add("legalForm", record.get("legal_form", None), lang="bos")
        entity.add("registrationNumber", record["registration_number"])
        entity.add("registrationNumber", record.get("unique_id", None))
        entity.add("description", record.get("customs_number", None), lang="eng")

        entity.add("sourceUrl", record["details_url"])
        entity.add("modifiedAt", record["date_of_last_decision"])
        entity.add("retrievedAt", datetime.datetime.now().isoformat())
        context.emit(entity)

        for person in founders_people:
            if person["name"] in FOUNDER_DENY_LIST:
                continue
            founder = context.make("Person")
            founder.id = context.make_id("BAFounder", entity.id, person["name"])
            founder.add("name", person["name"], lang="bos")
            context.emit(founder)

            own = context.make("Ownership")
            own.id = context.make_id("BAOwnership", entity.id, founder.id)
            own.add("asset", entity.id)
            own.add("owner", founder.id)
            own.add("ownershipType", "Founder", lang="eng")

            own.add("sharesValue", person["capital_paid"])
            if person["shares"].replace("\xa0", " ").strip("	 -"):
                own.add("sharesCount", person["shares"])
            context.emit(own)

        for comp in founders_companies:
            if comp["name"] in FOUNDER_DENY_LIST:
                continue
            if "dioničari" in comp["name"].lower():
                context.log.warning(
                    "Possible note instead of name (containing dioničari)",
                    name=comp["name"],
                    url=record["details_url"],
                )

            founder_company = context.make("LegalEntity")
            founder_company.id = context.make_id(
                "BAFounderCompany", entity.id, comp["name"]
            )
            founder_company.add("name", comp["name"], lang="bos")
            if comp.get("country"):
                founder_company.add("country", comp["country"], lang="bos")

            if comp.get("address"):
                founder_company.add("address", comp["address"], lang="bos")

            if comp.get("registration_number"):
                founder_company.add("registrationNumber", comp["registration_number"])

            context.emit(founder_company)

            own = context.make("Ownership")
            own.id = context.make_id("BAOwnership", entity.id, founder_company.id)
            own.add("asset", entity.id)
            own.add("owner", founder_company.id)
            own.add("ownershipType", "Founder", lang="eng")

            context.emit(own)

        for manager in managers:
            director = context.make("Person")
            director.id = context.make_id("BAdirector", entity.id, manager["name"])
            director.add("name", manager["name"], lang="bos")
            context.emit(director)

            rel = context.make("Directorship")
            rel.id = context.make_id("BADirectorship", entity.id, director.id)
            rel.add("role", manager["position"], lang="bos")
            rel.add("description", manager["authorizations"], lang="bos")
            rel.add("director", director)
            rel.add("organization", entity)

            context.emit(rel)
        return True


def generate_periods(
    from_date: datetime.date, to_date: datetime.date, step_months: int = 6
) -> List[Tuple[str, str]]:
    """
    Generate periods for the given range of dates
    Args:
        from_date: The start date.
        to_date: The end date.
        step_months: The step in months.
    Returns:
        The list of periods as a list of tuples.
    """

    periods = []
    current_date = from_date
    while current_date < to_date:
        next_date = current_date + datetime.timedelta(days=step_months * 30)
        if next_date > datetime.date.today():
            periods.append((current_date.strftime("%d/%m/%Y"), ""))
        else:
            periods.append(
                (current_date.strftime("%d/%m/%Y"), next_date.strftime("%d/%m/%Y"))
            )
        current_date = next_date

    return periods


def crawl(context: Context):
    """
    Main function to crawl and process data from the Registers of business entities in
    Bosnia and Herzegovina
    """

    secret_param = get_secret_param(context)

    if not secret_param:
        return

    cities = seed_city(context, secret_param)

    periods: List[Tuple[str, str]] = [
        # There are no companies registered before 2000
        ("01/01/2000", "01/01/2010"),
        # But some regions registered more than 500 companies
        # in one years, so we need to go all way down to 6 months step
    ] + generate_periods(
        from_date=datetime.date(2010, 1, 1),
        to_date=datetime.date.today(),
        step_months=6,
    )

    total = 0
    error_count = 0

    for city in cities:
        # Lets grab try to grab all records to see if it's less than 500
        new_recs = parse_city(
            context=context,
            city=city,
            secret_param=secret_param,
            from_date="",
            to_date="",
        )

        if len(new_recs) == 500:
            for period in periods:
                new_recs = parse_city(
                    context=context,
                    city=city,
                    secret_param=secret_param,
                    from_date=period[0],
                    to_date=period[1],
                )
                context.log.debug(
                    f'{city["city"]}, {period[0]}-{period[1]}: {len(new_recs)}'
                )
                total += len(new_recs)

                for rec in new_recs:
                    crawl_details(context, rec)

        else:
            context.log.debug(f'{city["city"]}, all the time: {len(new_recs)}')
            total += len(new_recs)

            for rec in new_recs:
                if not crawl_details(context, rec):
                    error_count += 1
    assert error_count < EXPECTED_ERRORS, f"Too many errors: {error_count}"
