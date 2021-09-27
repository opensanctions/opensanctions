import os
import string
from pprint import pprint  # noqa
from urllib.parse import urljoin

from opensanctions.helpers import make_address, apply_address, make_sanction


class RateLimit(Exception):
    pass


API_KEY = os.environ.get("OPENSANCTIONS_COH_API_KEY")
AUTH = (API_KEY, "")
SEARCH_URL = "https://api.companieshouse.gov.uk/search/disqualified-officers"
API_URL = "https://api.companieshouse.gov.uk/"
WEB_URL = "https://beta.companieshouse.gov.uk/register-of-disqualifications/A"


def http_get(context, url, params=None):
    res = context.http.get(url, params=params, auth=AUTH)
    if res.status_code == 429:
        raise RateLimit()
    return res


def crawl_item(context, listing):
    links = listing.get("links", {})
    url = urljoin(API_URL, links.get("self"))
    res = http_get(context, url)
    data = res.json()
    person = context.make("Person")
    _, officer_id = url.rsplit("/", 1)
    person.id = context.make_slug(officer_id)

    person.add("name", listing.get("title"))
    person.add("notes", listing.get("description"))
    source_url = urljoin(WEB_URL, links.get("self"))
    person.add("sourceUrl", source_url)

    last_name = data.pop("surname", None)
    person.add("lastName", last_name)
    forename = data.pop("forename", None)
    person.add("firstName", forename)
    other_forenames = data.pop("other_forenames", None)
    person.add("middleName", other_forenames)
    person.add("title", data.pop("title", None))

    person.add("nationality", data.pop("nationality", None))
    person.add("birthDate", data.pop("date_of_birth", None))
    person.add("topics", "crime")

    address = listing.get("address", {})
    address = make_address(
        context,
        full=listing.get("address_snippet"),
        street=address.get("address_line_1"),
        street2=address.get("premises"),
        city=address.get("locality"),
        postal_code=address.get("postal_code"),
        region=address.get("region"),
        # country_code=person.first("nationality"),
    )
    apply_address(context, person, address)

    for disqual in data.pop("disqualifications", []):
        case_id = disqual.get("case_identifier")
        sanction = make_sanction(context, person, key=case_id)
        sanction.add("recordId", case_id)
        sanction.add("startDate", disqual.get("disqualified_from"))
        sanction.add("endDate", disqual.get("disqualified_until"))
        sanction.add("listingDate", disqual.get("undertaken_on"))
        for key, value in disqual.get("reason", {}).items():
            value = value.replace("-", " ")
            reason = f"{key}: {value}"
            sanction.add("reason", reason)
        sanction.add("country", "gb")
        context.emit(sanction)

        for company_name in disqual.get("company_names", []):
            company = context.make("Company")
            company.id = context.make_slug("named", company_name)
            company.add("name", company_name)
            company.add("jurisdiction", "gb")
            context.emit(company)

            directorship = context.make("Directorship")
            directorship.id = context.make_id(person.id, company.id)
            directorship.add("director", person)
            directorship.add("organization", company)
            context.emit(directorship)

    context.emit(person, target=True)


def crawl(context):
    if API_KEY is None:
        context.log.error("Please set $OPENSANCTIONS_COH_API_KEY.")
        return
    try:
        for letter in string.ascii_uppercase:
            start_index = 0
            while True:
                params = {
                    "q": letter,
                    "start_index": start_index,
                    "items_per_page": 100,
                }
                res = http_get(context, SEARCH_URL, params=params)
                if res.status_code == 416:
                    break
                data = res.json()
                for item in data.pop("items", []):
                    crawl_item(context, item)
                start_index = data["start_index"] + data["items_per_page"]
                if data["total_results"] < start_index:
                    break
    except RateLimit:
        context.log.info("Rate limit exceeded, aborting.")
