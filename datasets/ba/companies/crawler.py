import datetime
import re
from urllib.parse import urljoin

import requests

from zavod import Context
from zavod import helpers as h

# Known failure mode: detail links the registry itself emits broken.
#
# The listing page builds each detail link by URL-encoding the company name into
# the APEX `f?p=` argument list, but its encoder stops at the first colon in the
# name and emits the remainder raw, e.g. for
# 'Društvo za proizvodnju, trgovinu, usluge i zastupanje "TEPIH SAN" d.o.o: Busovača':
#   f?p=186:13:<session>::NO:13:P13_P_POS_ID,P13_XMBS,P13_NAZIV:21054781%2C51-01-0090-08%2C%5CDru%C5%A1tvo%20...%20d.o.o: Busovača\&cs=3BB92596...
# `f?p=` is itself colon-delimited, so the raw colon starts a new argument, the
# name arrives truncated and APEX answers with a "Session state protection
# violation: This may be caused by manual alteration of a URL containing a
# checksum" page. The `&cs=` checksum is generated server-side over the intact
# name, and it is mandatory: dropping it, re-encoding the colon as %3A, quoting
# the whole argument or POSTing `p`/`cs` all produce the same violation. These
# links therefore cannot be repaired from our side, and are logged at info level
# rather than as warnings. The affected companies are still emitted from their
# listing row, but without legal form, status, JIB, founders or managers.
# 459 of 53,382 records in the 2026-08-21 run.
#
# Separately, a handful of names containing "&" are rejected before they reach
# APEX by the registry's web application firewall, which answers
# "The requested URL was rejected. [...] Your support ID is: ..." for the
# fully-encoded URL over both GET and POST, while other "&" names go through
# untouched (7 records in the same run). That is also outside our control, but
# it is rare and the rule may change, so it stays at warning level.
EXPECTED_ERRORS = 100

# A well-formed detail link carries eight colon-delimited APEX arguments - app,
# page, session, request, debug, clear cache, item names, item values - and
# therefore seven colons. Any extra colon comes from an unescaped one in the
# company name.
APEX_ARG_COLONS = 7

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
    r"^[\"(]?BRISAN iz sudskog registra[:)\"]?",
    r"^[\"(]?BRISAN(?: USLJED PRIPAJANJA)?[:)\"]?",
    r"- BRISAN \w+$",  # "Deleted" and one word
    r"^\"BRISAN ZBOG ZAKLJUČENJA LIKVIDACIJE[:)\"]?",
    r"\(PRESTANAK\),? u likvidaciji",
    r"\(u LIKVIDACIJI\)",
    r"\(?U STEČAJU[:)\"]?$",
]
SPLITS = [
    "(skraćeni naziv:",
    ", skraćeni naziv:",
    "skraćena oznaka firme:",
    "skraćeno:",
    "LOCAL COMMUNITY",
    "LOCAL COMUNITY",  # their typo - usually a translation, but I think it works to just split.
]
REMOVE_REGEX = re.compile("|".join(REMOVE_PATTERNS), flags=re.IGNORECASE)


def roughly_valid_regno(regno: str) -> bool:
    """
    Check if the registration number is valid
    Args:
        regno: The registration number to check.
    Returns:
        True if the registration number is valid, False otherwise.
    """
    return bool(REGEX_ROUGH_REGNO.match(regno))


def apex_url(url: str) -> str:
    """Percent-encode ampersands inside the APEX `f?p=` parameter value.

    The registry embeds the company name in the `p=` parameter, so a name
    containing "&" splits the query string when the URL is re-encoded on its
    way to the server, and the request comes back as a 400.
    """
    prefix, sep, checksum = url.rpartition("&cs=")
    if not sep:
        return url
    return f"{prefix.replace('&', '%26')}{sep}{checksum}"


def apex_link_broken(url: str) -> bool:
    """Check whether the registry left an unescaped colon in a detail link.

    The company name is the last APEX argument, so a colon the registry failed to
    encode adds an argument to the `f?p=` list. Such a link always fails the
    registry's session state protection check and cannot be repaired; see the note
    at the top of this file.

    Args:
        url: The details URL taken from the listing page.
    Returns:
        True if the link carries an unescaped colon, False otherwise.
    """
    prefix, _, _ = url.rpartition("&cs=")
    args = (prefix or url).partition("f?p=")[2]
    return args.count(":") > APEX_ARG_COLONS


def get_secret_param(context: Context) -> str:
    """
    Goes through the chain of redirects to get the secret param.

    The whole crawl hangs off this session identifier, so a failure here is
    structural: it is raised rather than logged, so that an unreachable registry
    fails the run at the point of failure instead of emitting an empty dataset.

    Args:
        context: The context object for the current dataset.
    Returns:
        The secret param as a str.
    """
    resp = context.fetch_text(context.data_url)
    assert resp is not None, context.data_url
    matches = re.search(r"f\?p=18\d\:\d+\:(\d+)", resp)
    assert matches is not None, f"Cannot find secret param at {context.data_url}"
    return matches.group(1)


def clean_name(raw_name: str | None) -> list[str]:
    """
    Clean a single company name string, returning one name and any aliases found.

    If the input is None or empty, returns [None].
    """
    if not raw_name:
        return []
    cleaned = REMOVE_REGEX.sub("", raw_name).strip(" -:()")
    names = h.multi_split(cleaned, SPLITS)
    return names


def seed_city(context: Context, secret_param: str) -> list[dict[str, str]]:
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
    resp = context.fetch_text(url=DICTS_URL, method="POST", data=payload)
    assert resp is not None
    cities = re.findall(r'id: (\d+), data: "([\w /-]+)"', resp)

    return [{"city": city, "code": code} for code, city in cities]


def parse_city(
    context: Context,
    secret_param: str,
    city: dict[str, str],
    from_date: str,
    to_date: str,
) -> list[dict[str, str | None]]:
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

    context.fetch_text(url=TOUCH_URL, method="POST", data=TOUCH_PAYLOAD1)

    context.fetch_text(url=TOUCH_URL, method="POST", data=TOUCH_PAYLOAD2)

    result = context.fetch_html(url=RETRIEVE_URL, method="POST", data=RETRIEVE_PAYLOAD)

    rows = result.findall(".//tr")

    records = []
    for row in rows:
        if row.find(".//td/a") is None:
            continue
        record: dict[str, str | None] = {}
        record["city"] = city.get("city", "")
        record["registration_number"] = row[0].text
        record["name"] = row[1][0].text
        record["abbreviation"] = row[2][0].text
        record["address"] = row[3].text
        record["date_of_last_decision"] = row[4].text
        detail_hrefs = h.xpath_strings(row, ".//td/a/@href")
        record["details_url"] = (
            apex_url(urljoin(BASE_URL, detail_hrefs[0])) if detail_hrefs else None
        )

        records.append(record)

    return records


def crawl_details(context: Context, record: dict[str, str | None]) -> bool:
    """
    Fetches and emits the details of a company from the website.

    Returns False if an error occurred, True otherwise.

    Args:
        context: The context object for the current dataset.
        record: The record to fetch the details for.
    """
    details_url = record["details_url"]
    assert details_url is not None
    try:
        details_page = context.fetch_html(details_url, cache_days=CACHE_DAYS)
    except requests.exceptions.HTTPError as exc:
        context.log.warning(
            f"Failed to fetch company {details_url}: {type(exc)}, {exc}"
        )
        return False

    legal_form = h.xpath_strings(
        details_page,
        '//td[contains(text(), "Legal form of organization")]/following-sibling::td/text()',
    )
    if legal_form:
        record["legal_form"] = legal_form[0]

    address_add = h.xpath_strings(
        details_page,
        '//td[contains(text(), "Address")]/following-sibling::td/text()',
    )
    if address_add:
        record["address_additional"] = address_add[0]

    status = h.xpath_strings(
        details_page,
        '//td[contains(text(), "Status (Bankruptcy – YES/NO)")]/following-sibling::td/text()',
    )
    if status:
        record["status_bankruptcy"] = status[0]

    uin = h.xpath_strings(
        details_page,
        '//td[contains(text(), "Unique Identification Number")]/following-sibling::td/text()',
    )
    # Jedinstveni identifikacioni broj - JIB or UIN
    # https://www.vatify.eu/bosnia-and-herzegovina-vat-number.html
    if uin:
        record["unique_id"] = uin[0]

    customs_number = h.xpath_strings(
        details_page,
        '//td[contains(text(), "Customs Number")]/following-sibling::td/text()',
    )
    if customs_number and customs_number[0].replace("\xa0", " ").strip():
        record["customs_number"] = f"Customs number: {customs_number[0]}"

    founders_people: list[dict[str, str]] = []
    founders_companies: list[dict[str, str]] = []
    managers: list[dict[str, str]] = []

    try:
        founders_hrefs = h.xpath_strings(details_page, '//*[@id="podmeni"]/p/a/@href')
        founders_url = urljoin(BASE_URL, founders_hrefs[0])
    except IndexError:
        if apex_link_broken(details_url):
            context.log.info(
                "Details page unreachable: registry link has an unescaped colon",
                url=details_url,
            )
        else:
            context.log.warning("Details page empty", url=details_url)
    else:
        founders_page = context.fetch_html(founders_url)

        names = h.xpath_strings(
            founders_page,
            '//td[contains(text(), "Ime osnivača")]/following-sibling::td/text()',
        )
        cap_paid = h.xpath_strings(
            founders_page,
            '//td[contains(text(), "Kapital [uplaćeni]")]/following-sibling::td/text()',
        )
        shares = h.xpath_strings(
            founders_page,
            '//td[contains(text(), "Dionice [broj]")]/following-sibling::td/text()',
        )
        basic_data = h.xpath_strings(
            founders_page,
            '//td[contains(text(), "Basic data")]/following-sibling::td/text()',
        )
        reg_num = h.xpath_strings(
            founders_page,
            '//td[contains(text(), "Registration Number")]/following-sibling::td/text()',
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
            comp_address = ", ".join(parsed_bd[1:]).strip().strip(",")
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
                    "address": comp_address,
                    "country": country,
                }
            )

        managers_hrefs = h.xpath_strings(founders_page, '//*[@id="podmeni"]/p/a/@href')
        managers_url = urljoin(BASE_URL, managers_hrefs[1])
        managers_page = context.fetch_html(managers_url)
        managers_names = h.xpath_strings(
            managers_page,
            '//td[contains(text(), "Name")]/following-sibling::td/text()',
        )
        managers_pos = h.xpath_strings(
            managers_page,
            '//td[contains(text(), "Position")]/following-sibling::td/text()',
        )[1::2]
        managers_auth = h.xpath_strings(
            managers_page,
            '//td[contains(text(), "Authorisations/ Position limits")]/following-sibling::td/text()',
        )
        if managers_names:
            for i, mgr_name in enumerate(managers_names):
                managers.append(
                    {
                        "name": mgr_name,
                        "authorizations": managers_auth[i],
                        "position": managers_pos[i],
                    }
                )

    finally:
        entity = context.make("Company")
        reg_number = record.get("registration_number")
        if reg_number is not None and roughly_valid_regno(reg_number):
            entity.id = context.make_slug(reg_number)
        else:
            assert record["name"] is not None
            entity.id = context.make_id("BACompany", record["name"])

        # Abbreviation isn't so much an alias as simply a shortened but totally valid form
        # name:   MIDAX d.o.o. za proizvodnju, promet i usluge Banovići
        # google translate:
        #         MIDAX d.o.o. for production, trade and services Banovići
        # abbrev: MIDAX d.o.o. Banovići
        for raw_name in [record["name"], record["abbreviation"]]:
            if names := clean_name(raw_name):
                entity.add("name", names[0], lang="bos")
                entity.add("alias", names[1:], lang="bos")
        if not entity.has("name"):
            context.log.warning("No valid name found", url=record["details_url"])
            return True
        entity.add("status", record.get("status_bankruptcy", None), lang="bos")

        entity.add("country", "ba")
        address: str | None = record["address"]
        if address != "-":
            entity.add("address", address, lang="bos")
        entity.add("address", record.get("address_additional", None), lang="bos")

        entity.add("legalForm", record.get("legal_form", None), lang="bos")
        entity.add("registrationNumber", record["registration_number"])
        entity.add("registrationNumber", record.get("unique_id", None))
        entity.add("description", record.get("customs_number", None), lang="eng")

        entity.add("sourceUrl", record["details_url"])
        entity.add("modifiedAt", record["date_of_last_decision"])
        context.emit(entity)

        for person in founders_people:
            if person["name"] in FOUNDER_DENY_LIST:
                continue
            founder = context.make("Person")
            founder.id = context.make_id("BAFounder", entity.id, person["name"])
            founder.add("name", person["name"], lang="bos")
            if not founder.has("name"):
                context.log.info(
                    "Skipping founder without a usable name",
                    name=person["name"],
                    url=record["details_url"],
                )
                continue
            context.emit(founder)

            own = context.make("Ownership")
            own.id = context.make_id("BAOwnership", entity.id, founder.id)
            own.add("asset", entity.id)
            own.add("owner", founder.id)
            own.add("role", "Founder", lang="eng")

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
            if not founder_company.has("name"):
                context.log.info(
                    "Skipping founder company without a usable name",
                    name=comp["name"],
                    url=record["details_url"],
                )
                continue
            if comp.get("country"):
                founder_company.add("country", comp["country"], lang="bos")

            if comp.get("address") and comp["address"] != "-":
                founder_company.add("address", comp["address"], lang="bos")

            if comp.get("registration_number"):
                founder_company.add("registrationNumber", comp["registration_number"])

            context.emit(founder_company)

            own = context.make("Ownership")
            own.id = context.make_id("BAOwnership", entity.id, founder_company.id)
            own.add("asset", entity.id)
            own.add("owner", founder_company.id)
            own.add("role", "Founder", lang="eng")

            context.emit(own)

        for manager in managers:
            director = context.make("Person")
            director.id = context.make_id("BAdirector", entity.id, manager["name"])
            director.add("name", manager["name"], lang="bos")
            if not director.has("name"):
                context.log.info(
                    "Skipping director without a usable name",
                    name=manager["name"],
                    url=record["details_url"],
                )
                continue
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
) -> list[tuple[str, str]]:
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


def crawl(context: Context) -> None:
    """
    Main function to crawl and process data from the Registers of business entities in
    Bosnia and Herzegovina
    """

    secret_param = get_secret_param(context)
    cities = seed_city(context, secret_param)

    periods: list[tuple[str, str]] = [
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
                    f"{city['city']}, {period[0]}-{period[1]}: {len(new_recs)}"
                )
                total += len(new_recs)

                for rec in new_recs:
                    if not crawl_details(context, rec):
                        error_count += 1
        else:
            context.log.debug(f"{city['city']}, all the time: {len(new_recs)}")
            total += len(new_recs)

            for rec in new_recs:
                if not crawl_details(context, rec):
                    error_count += 1
    assert error_count < EXPECTED_ERRORS, f"Too many errors: {error_count}"
