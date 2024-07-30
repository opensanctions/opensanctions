import time
from itertools import count
from requests.exceptions import HTTPError, RetryError
from typing import Any, Dict, Generator, Optional
from zavod import Context, helpers as h

SLEEP = 5
DATE_FORMAT = ["%m/%d/%Y"]


def http_get(
    context: Context,
    url,
    cache_days: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    for attempt in count(1):
        try:
            return context.fetch_json(url, cache_days=cache_days)
        except (RetryError, HTTPError) as err:
            if isinstance(err, RetryError) or err.response.status_code == 429:
                if attempt > 5:
                    raise RuntimeError("Rate limit exceeded.")
                context.log.warn(
                    f"Rate limit exceeded, sleeping {SLEEP}s...",
                    error=str(err),
                )
                time.sleep(SLEEP * 5)
            else:
                context.log.exception("Failed to fetch data: %s" % url)
                return None


def fetch_registrants(context: Context) -> Generator[dict, None, None]:
    # ** Loop through both Active and Terminated registrants **
    for status in ("Active", "Terminated"):
        url = f"https://efile.fara.gov/api/v1/Registrants/json/{status}"
        data = http_get(context, url, cache_days=1)
        # Check if data is a dictionary
        if not isinstance(data, dict):
            context.log.error("Response data is not in the expected dictionary format.")
            return

        registrants = data.get("REGISTRANTS_ACTIVE")
        registrants = registrants or data.get("REGISTRANTS_TERMINATED")
        assert len(registrants["ROW"]) > 0, "No registrants found in the response."
        for item in registrants["ROW"]:
            assert isinstance(
                item, dict
            ), "Registrant data is not in the expected format."
            yield item


def fetch_principals(
    context: Context, registration_number: Optional[str]
) -> Generator[dict, None, None]:
    """Fetch principal information for a given registration number."""
    # ** Loop through both Active and Terminated principals links **

    for status, days in (("Active", 5), ("Terminated", 180)):
        url = f"https://efile.fara.gov/api/v1/ForeignPrincipals/json/{status}/{registration_number}"
        data = http_get(context, url, cache_days=1)
        if not isinstance(data, dict):
            context.log.error(f"Failed to fetch data for principals: {url}.")
            return

        principals = data.get("FOREIGNPRINCIPALS_ACTIVE")
        principals = principals or data.get("FOREIGNPRINCIPALS_TERMINATED")
        if isinstance(principals, str):
            return
        rows = principals.get("ROW", [])
        if isinstance(rows, dict):
            rows = [rows]
        if not len(rows) > 0:
            msg = f"No principals found for reg number {registration_number}."
            context.log.info(msg)
            return

        for item in rows:
            yield item


def crawl(context: Context) -> None:
    for item in fetch_registrants(context):
        # Extract relevant fields from each item
        address = h.make_address(
            context,
            street=item.pop("Address_1", None),
            street2=item.pop("Address_2", None),
            city=item.pop("City", None),
            postal_code=item.pop("Zip", None),
            state=item.pop("State", None),
            country_code="us",
        )
        registration_date = h.parse_date(item.pop("Registration_Date"), DATE_FORMAT)
        registration_number = item.pop("Registration_Number")

        # Create a Company entity for each item
        company = context.make("Company")
        company.id = context.make_slug(
            "reg", registration_number
        )  # Create a unique ID based on name and address
        company.add("name", item.pop("Name"))
        h.copy_address(company, address)
        company.add("incorporationDate", registration_date)
        company.add("registrationNumber", registration_number)

        context.audit_data(item)
        context.emit(company)

        context.log.info(f"Fetching principals for {registration_number}...")

        # Fetch agency client information
        for principal_item in fetch_principals(context, registration_number):
            # Add relevant agency client information to the company entity
            p_name = principal_item.pop("Foreign_principal")
            address = h.make_address(
                context,
                street=principal_item.pop("Address_1", None),
                street2=principal_item.pop("Address_2", None),
                city=principal_item.pop("City", None),
                postal_code=principal_item.pop("Zip", None),
                state=principal_item.pop("State", None),
                country=principal_item.pop("Country_location_represented", None),
            )

            # Now create a new Company entity for the agency client
            principal = context.make("LegalEntity")
            principal.id = context.make_id(registration_number, p_name)
            principal.add("name", p_name)
            h.copy_address(principal, address)

            # Emit the new agency client entity
            context.emit(principal)
            # Create a relationship between the company and the agency client
            representation = context.make("Representation")
            representation.id = context.make_id("rep", company.id, principal.id)
            representation.add("agent", company)
            representation.add("client", principal)

            p_registration_date = h.parse_date(
                principal_item.pop("Foreign_principal_registration_date", None),
                DATE_FORMAT,
            )
            p_termination_date = h.parse_date(
                principal_item.pop("Foreign_principal_termination_date", None),
                DATE_FORMAT,
            )
            representation.add("startDate", p_registration_date)
            representation.add("endDate", p_termination_date)

            context.audit_data(
                principal_item,
                [
                    "Registration_number",
                    "ROWNUM",
                    "Registrant_name",
                    "Registration_date",
                ],
            )
            context.emit(representation)

        time.sleep(SLEEP)
