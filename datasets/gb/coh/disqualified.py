import os
import re
import string
from itertools import count
from typing import Dict, Any, Optional
from urllib.parse import urljoin

from requests.exceptions import HTTPError, RetryError

import time
from zavod import Context, Entity
from zavod import helpers as h


class AbortCrawl(Exception):
    pass


API_KEY = os.environ.get("OPENSANCTIONS_COH_API_KEY", "")
AUTH = (API_KEY, "")
API_URL = "https://api.company-information.service.gov.uk/"
WEB_URL = "https://find-and-update.company-information.service.gov.uk/register-of-disqualifications/A"
SLEEP = 315


def http_get(
    context: Context,
    url,
    params: Optional[Dict[str, Any]] = None,
    cache_days: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    for attempt in count(1):
        try:
            return context.fetch_json(
                url,
                params=params,
                auth=AUTH,
                cache_days=cache_days,
            )
        except (RetryError, HTTPError) as err:
            if isinstance(err, RetryError) or err.response.status_code == 429:
                if attempt > 5:
                    raise AbortCrawl()
                context.log.warn(
                    f"Rate limit exceeded, sleeping {SLEEP}s...",
                    error=str(err),
                )
                time.sleep(SLEEP)
            else:
                context.log.exception("Failed to fetch data: %s" % url)
                return None


def build_address(
    context: Context, full: str, address_data: dict[str, Optional[str]]
) -> Optional[Entity]:
    address_components = {
        "street": address_data.get("address_line_1"),
        "street2": address_data.get("premises"),
        "city": address_data.get("locality"),
        "region": address_data.get("region"),
    }

    # If some address components are available, they build an actual (partial) address string from them.
    # But when no address components are available, this is what they helpfully set the full address to.
    if (
        full
        != "Not Available, Not Available, Not Available, Not Available, NOT AVAILABLE"
    ):
        address_components["full"] = full

    # Sometimes the PO Box is in the postal code field
    postal_code = address_data.get("postal_code")
    if postal_code:
        po_box_match = re.match(
            r"^P(\.?)O(\.?) Box (?P<po_box>.+)", postal_code, re.IGNORECASE
        )
        if po_box_match:
            address_components["po_box"] = po_box_match.group("po_box")
        else:
            address_components["postal_code"] = postal_code

    cleaned_address_components = {
        k: v
        for k, v in address_components.items()
        if v is not None and v.lower() != "not available"
    }
    # If no components are available, don't return an Address entity
    if not cleaned_address_components:
        return None

    return h.make_address(context, **cleaned_address_components)


def resolve_company_name_by_number(
    context: Context, company_number: str
) -> Optional[str]:
    """If the company_number is found on Companies House, return its name, else None."""
    search_url = f"https://find-and-update.company-information.service.gov.uk/search?q={company_number}"
    doc = context.fetch_html(search_url, cache_days=7)
    if len(doc.xpath(".//div[@id='no-results']")) > 0:
        return None
    results = doc.xpath(".//ul[@class='results-list']//a")
    assert len(results) > 0
    for link in results:
        if f"/company/{company_number}" in link.get("href"):
            return link.text_content().strip()
    return None


def crawl_item(context: Context, listing: Dict[str, Any]) -> None:
    links = listing.get("links", {})
    url = urljoin(API_URL, links.get("self"))
    data = http_get(context, url, cache_days=45)
    if data is None:
        return

    is_corporate = "/disqualified-officers/corporate/" in url
    person = context.make("Organization" if is_corporate else "Person")

    _, officer_id = url.rsplit("/", 1)
    person.id = context.make_slug(officer_id)

    person.add("name", listing.get("title"))
    description = listing.get("description", "")
    if description is not None and len(description) > 0:
        for desc in description.split(" - "):
            desc = context.lookup_value("description", desc) or desc.strip()
            if desc.startswith("Born on "):
                _, dob = desc.split("Born on ", 1)
                person.add_schema("Person")
                h.apply_date(person, "birthDate", dob.strip())
            else:
                person.add("notes", desc)
    person.add("topics", "corp.disqual")
    source_url = urljoin(WEB_URL, links.get("self"))
    person.add("sourceUrl", source_url)

    # TODO(Leon Handreke): Clean this up, it seems they added lots of Companies to this list of persons.
    nationality = data.pop("nationality", "")
    person.add_cast("Person", "birthDate", data.pop("date_of_birth", None))
    if person.schema.is_a("Person"):
        h.apply_name(
            person,
            first_name=data.pop("forename", None),
            last_name=data.pop("surname", None),
            middle_name=data.pop("other_forenames", None),
            lang="eng",
        )
        person.add("nationality", nationality.split(","))

    else:
        person.add("name", data.pop("forename", None))
        person.add("name", data.pop("surname", None))
        person.add("name", data.pop("other_forenames", None))
        person.add("country", nationality.split(","))

    person.add_cast("Person", "title", data.pop("title", None))

    address = build_address(
        context,
        full=listing.get("address_snippet"),
        address_data=listing.get("address", {}) or {},
    )
    if address is not None:
        h.copy_address(person, address)

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

        address = build_address(
            context,
            full=listing.get("address_snippet"),
            address_data=disqual.get("address", {}) or {},
        )

        for company_name in disqual.get("company_names", []):
            # If company_name is numeric, search for the company by number
            if company_name.isdigit():
                resolved_name = resolve_company_name_by_number(context, company_name)
                if resolved_name:
                    company_name = resolved_name
                else:
                    context.log.info(
                        f"Skipping numeric company with no match: {company_name}"
                    )
                    continue
            company = context.make("Company")
            if not company_name:
                context.log.info(
                    f"Skipping company with no name for person {person.id}",
                    company_name=company_name,
                    person_id=person.id,
                )
                continue
            company.id = context.make_slug("named", company_name)
            company.add("name", company_name)
            company.add("jurisdiction", "gb")
            # company.add("topics", "crime")
            if address is not None:
                h.copy_address(company, address)
            context.emit(company)

            directorship = context.make("Directorship")
            directorship.id = context.make_id(person.id, company.id)
            directorship.add("director", person)
            directorship.add("organization", company)
            context.emit(directorship)

    context.emit(person)


def crawl(context: Context) -> None:
    if not len(API_KEY):
        context.log.error("Please set $OPENSANCTIONS_COH_API_KEY.")
        return
    try:
        for letter in string.ascii_uppercase:
            start_index = 0
            while True:
                params = {
                    "q": letter,
                    "start_index": str(start_index),
                    "items_per_page": "100",
                }
                data = http_get(context, context.data_url, params=params, cache_days=1)
                if data is None:
                    break
                context.log.info("Search: %s" % letter, start_index=start_index)
                for item in data.pop("items", []):
                    crawl_item(context, item)
                start_index = data["start_index"] + data["items_per_page"]
                if data["total_results"] < start_index or start_index >= 1000:
                    break
    except AbortCrawl:
        context.log.info("Rate limit exceeded, aborting.")
