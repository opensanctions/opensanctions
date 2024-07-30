import time
from requests.exceptions import HTTPError
from typing import Generator, Optional
from zavod import Context, helpers as h

DATE_FORMAT = ["%m/%d/%Y"]


def fetch_registrants(context: Context) -> Generator[dict, None, None]:
    # ** Loop through both Active and Terminated registrants **
    status_links = [
        "https://efile.fara.gov/api/v1/Registrants/json/Active",
        "https://efile.fara.gov/api/v1/Registrants/json/Terminated",
    ]

    for link in status_links:
        # Use fetch_json to get the response
        try:
            data = context.fetch_json(link, cache_days=1)
        except HTTPError as err:
            context.log.error(f"Failed to fetch data from {link}: {err}")
            if err.response.status_code == 429:
                time.sleep(10)
            return

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
    """Fetch agency client information for a given registration number."""
    # ** Loop through both Active and Terminated agency client links **

    for status, days in (("Active", 5), ("Terminated", 180)):
        url = f"https://efile.fara.gov/api/v1/ForeignPrincipals/json/{status}/{registration_number}"
        try:
            data = context.fetch_json(url, cache_days=days)
        except HTTPError as err:
            context.log.error(f"Failed to fetch data for principals using URL: {url}.")
            if err.response.status_code == 429:
                time.sleep(10)
            return
        if not isinstance(data, dict):
            context.log.error(
                f"Failed to fetch data for agency client using URL: {url}."
            )
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

        # # Increment request count and wait
        # request_count += 1
        # if request_count >= max_entities_to_capture:
        #     context.log.info("Captured the maximum number of entities.")
        #     break  # Exit after capturing the first 5 entities

        # Wait for 15 seconds before the next request
        time.sleep(2)
