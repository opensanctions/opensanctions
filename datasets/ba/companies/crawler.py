from typing import Dict, List, Tuple
from urllib.parse import urljoin
import re
import datetime

from zavod import Context


# Unfortunatelly no cache for the listing page, as the state of the current
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
        The secret param as a str.
    """
    resp = context.fetch_text(context.data_url, cache_days=CACHE_DAYS)

    matches = re.search(r"f\?p=18\d\:\d+\:(\d+)", resp)

    if not matches:
        context.log.warning("Cannot find secret param")
        return ""
    else:
        return matches.group(1)


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
    details_page = context.fetch_html(record["details_url"])

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
        context.log.warning("Details page empty")
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

        entity.add("name", record["name"], lang="bos")
        entity.add("name", record["abbreviation"], lang="bos")
        if record.get("status_bankruptcy"):
            entity.add("status", record["status_bankruptcy"], lang="bos")

        entity.add("country", "ba")
        entity.add("address", record["address"], lang="bos")
        if "address_additional" in record:
            entity.add("address", record["address_additional"], lang="bos")

        entity.add("legalForm", record["legal_form"], lang="bos")
        entity.add("registrationNumber", record["registration_number"])

        if record.get("unique_id"):
            entity.add("registrationNumber", record["unique_id"])

        if record.get("customs_number"):
            entity.add("description", record["customs_number"], lang="eng")

        entity.add("sourceUrl", record["details_url"])
        entity.add("modifiedAt", record["date_of_last_decision"])
        entity.add("retrievedAt", datetime.datetime.now().isoformat())
        context.emit(entity, target=True)

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
                    name=name,
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
                crawl_details(context, rec)
