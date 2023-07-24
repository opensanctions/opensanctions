import os
import string
from typing import Optional, Dict, Any
from urllib.parse import urljoin
from requests.exceptions import HTTPError

from zavod import Context
from opensanctions import helpers as h


class AbortCrawl(Exception):
    pass


API_KEY = os.environ.get("OPENSANCTIONS_COH_API_KEY")
AUTH = (API_KEY, "")
SEARCH_URL = "https://api.companieshouse.gov.uk/search/disqualified-officers"
API_URL = "https://api.companieshouse.gov.uk/"
WEB_URL = "https://beta.companieshouse.gov.uk/register-of-disqualifications/A"


def http_get(
    context: Context,
    url,
    params: Optional[Dict[str, Any]] = None,
    cache_days: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    try:
        return context.fetch_json(
            url,
            params=params,
            auth=AUTH,
            cache_days=cache_days,
        )
    except HTTPError as err:
        if err.response.status_code in (429, 416):
            raise AbortCrawl()
        context.log.info("HTTP error: %r", err)
        return None


def crawl_item(context: Context, listing: Dict[str, Any]) -> None:
    links = listing.get("links", {})
    url = urljoin(API_URL, links.get("self"))
    data = http_get(context, url, cache_days=14)
    if data is None:
        context.log.warn("Could not fetch: %r" % url)
        return
    person = context.make("Person")
    _, officer_id = url.rsplit("/", 1)
    person.id = context.make_slug(officer_id)

    person.add("name", listing.get("title"))
    person.add("notes", listing.get("description"))
    person.add("topics", "crime")
    source_url = urljoin(WEB_URL, links.get("self"))
    person.add("sourceUrl", source_url)

    last_name = data.pop("surname", None)
    person.add("lastName", last_name)
    forename = data.pop("forename", None)
    person.add("firstName", forename)
    other_forenames = data.pop("other_forenames", None)
    person.add("middleName", other_forenames)
    person.add("title", data.pop("title", None))

    nationality = data.pop("nationality", None)
    if nationality is not None:
        person.add("nationality", nationality.split(","))
    person.add("birthDate", data.pop("date_of_birth", None))
    person.add("topics", "crime")

    address = listing.get("address", {})
    address = h.make_address(
        context,
        full=listing.get("address_snippet"),
        street=address.get("address_line_1"),
        street2=address.get("premises"),
        city=address.get("locality"),
        postal_code=address.get("postal_code"),
        region=address.get("region"),
        # country_code=person.first("nationality"),
    )
    h.apply_address(context, person, address)

    for disqual in data.pop("disqualifications", []):
        case_id = disqual.get("case_identifier")
        sanction = h.make_sanction(context, person, key=case_id)
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

        address = disqual.get("address", {})
        address = h.make_address(
            context,
            full=listing.get("address_snippet"),
            street=address.get("address_line_1"),
            street2=address.get("premises"),
            city=address.get("locality"),
            postal_code=address.get("postal_code"),
            region=address.get("region"),
            # country_code=person.first("nationality"),
        )

        for company_name in disqual.get("company_names", []):
            company = context.make("Company")
            company.id = context.make_slug("named", company_name)
            company.add("name", company_name)
            company.add("jurisdiction", "gb")
            company.add("topics", "crime")
            context.emit(company)
            h.apply_address(context, company, address)

            directorship = context.make("Directorship")
            directorship.id = context.make_id(person.id, company.id)
            directorship.add("director", person)
            directorship.add("organization", company)
            context.emit(directorship)

    context.emit(person, target=True)


def crawl(context: Context) -> None:
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
                data = http_get(context, SEARCH_URL, params=params)
                for item in data.pop("items", []):
                    crawl_item(context, item)
                start_index = data["start_index"] + data["items_per_page"]
                if data["total_results"] < start_index:
                    break
    except AbortCrawl:
        context.log.info("Rate limit exceeded, aborting.")
