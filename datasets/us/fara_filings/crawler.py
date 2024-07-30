import time
from typing import Any, Generator, Optional, Tuple, Union
from zavod import Context, helpers as h

DATE_FORMAT = ["%m/%d/%Y"]


def get_value(
    data: dict, keys: Tuple[str, ...], default: Optional[Any] = None
) -> Union[Optional[Any], Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


def parse_json(context: Context) -> Generator[dict, None, None]:
    # ** Loop through both Active and Terminated registrants **
    status_links = [
        "https://efile.fara.gov/api/v1/Registrants/json/Active",
        "https://efile.fara.gov/api/v1/Registrants/json/Terminated",
    ]

    for link in status_links:
        response = context.http.get(link)

        # Check the response status
        if response.status_code == 200:
            data = response.json()
            # ** Update parsing logic for both Active and Terminated registrants **
            if "REGISTRANTS_ACTIVE" in data and "ROW" in data["REGISTRANTS_ACTIVE"]:
                registrants = data["REGISTRANTS_ACTIVE"]["ROW"]
                context.log.info(f"Fetched {len(registrants)} results from {link}.")
                for item in registrants:
                    yield item

            elif "REGISTRANTS_TERMINATED" in data:
                # Handling for Terminated registrants (check list format)
                terminated_data = data["REGISTRANTS_TERMINATED"].get("ROW")
                if isinstance(terminated_data, list):
                    context.log.info(
                        f"Fetched {len(terminated_data)} terminated results from {link}."
                    )
                    for item in terminated_data:
                        yield item
                else:
                    # If ROW is not a list, yield directly if it's a dictionary
                    context.log.info(
                        "No detailed terminated records found. Check the structure."
                    )
                    yield terminated_data

            else:
                context.log.info("No data found in the expected format.")
        elif response.status_code == 400:
            context.log.error(f"Bad request: {response.status_code}, {response.text}")
        elif response.status_code == 429:
            context.log.error(
                f"Too Many Requests: {response.status_code}, {response.text}"
            )
            time.sleep(10)  # Wait for the window to reset
        elif response.status_code == 500:
            context.log.error(f"Server error: {response.status_code}, {response.text}")
        else:
            context.log.error(
                f"Unexpected status code: {response.status_code}, {response.text}"
            )


def get_agency_client(
    context: Context, registration_number: Optional[str]
) -> Optional[dict]:
    """Fetch agency client information for a given registration number."""
    # ** Loop through both Active and Terminated agency client links **
    agency_links = [
        f"https://efile.fara.gov/api/v1/ForeignPrincipals/json/Active/{registration_number}",
        f"https://efile.fara.gov/api/v1/ForeignPrincipals/json/Terminated/{registration_number}",
    ]

    for agency_url in agency_links:
        response = context.http.get(agency_url)
        if response.status_code == 200:
            data = response.json()
            # Check if "FOREIGNPRINCIPALS_ACTIVE" and "ROW" keys exist
            if (
                "FOREIGNPRINCIPALS_ACTIVE" in data
                and "ROW" in data["FOREIGNPRINCIPALS_ACTIVE"]
            ):
                return data["FOREIGNPRINCIPALS_ACTIVE"]["ROW"]
            elif (
                "FOREIGNPRINCIPALS_TERMINATED" in data
                and "ROW" in data["FOREIGNPRINCIPALS_TERMINATED"]
            ):
                return data["FOREIGNPRINCIPALS_TERMINATED"]["ROW"]
            else:
                context.log.info(
                    f"No agency client found for registration number {registration_number}."
                )
                return None
        elif response.status_code == 400:
            context.log.error(
                f"Bad request for agency client: {response.status_code}, {response.text}"
            )
        elif response.status_code == 404:
            context.log.error(
                f"Agency client not found for registration number {registration_number}."
            )
        elif response.status_code == 500:
            context.log.error(
                f"Server error while fetching agency client: {response.status_code}, {response.text}"
            )
        # ** Break the loop and return if we found a valid response **
        return None
    return None


def crawl(context: Context) -> None:
    max_entities_to_capture = 1000  # Limit to the first entity
    request_count = 0

    for item in parse_json(context):
        # Extract relevant fields from each item
        name = item.get("Name")
        address1 = item.get("Address_1")
        address2 = item.get("Address_2")
        city = item.get("City")
        state = item.get("State")
        zip_code = item.get("Zip")
        address = h.make_address(
            context,
            street=address1,
            street2=address2,
            city=city,
            postal_code=zip_code,
            state=state,
        )
        registration_date = h.parse_date(item.get("Registration_Date"), DATE_FORMAT)
        registration_number = item.get("Registration_Number")

        # Create a Company entity for each item
        company = context.make("Company")
        company.id = context.make_slug(
            "reg", registration_number
        )  # Create a unique ID based on name and address
        company.add("name", name)
        company.add("address", address)
        company.add("incorporationDate", registration_date)
        company.add("registrationNumber", registration_number)

        # Fetch agency client information
        agency_client_info = get_agency_client(context, registration_number)
        if agency_client_info:
            # Add relevant agency client information to the company entity
            p_name = agency_client_info.get("Foreign_principal")
            p_zip_code = agency_client_info.get("Zip")
            p_registration_number = agency_client_info.get("Registration_number")
            p_address1 = agency_client_info.get("Address_1")
            p_address2 = agency_client_info.get("Address_2")
            p_city = agency_client_info.get("City")
            p_country = agency_client_info.get("Country_location_represented")
            address = h.make_address(
                context,
                street=p_address1,
                street2=p_address2,
                city=p_city,
                postal_code=p_zip_code,
                country=p_country,
            )
            p_registration_date = h.parse_date(
                agency_client_info.get("Foreign_principal_registration_date"),
                DATE_FORMAT,
            )
            p_termination_date = h.parse_date(
                agency_client_info.get("Foreign_principal_termination_date"),
                DATE_FORMAT,
            )

            # Now create a new Company entity for the agency client
            principal = context.make("LegalEntity")
            principal.id = context.make_id(registration_number, p_name)
            principal.add("name", p_name)
            principal.add("address", address)
            principal.add(
                "country",
                agency_client_info.get("Country_location_represented"),
            )
            principal.add("registrationNumber", p_registration_number)
            principal.add("incorporationDate", p_registration_date)

            # Emit the new agency client entity
            context.emit(principal)

        context.emit(company)

        if company and principal:
            # Create a relationship between the company and the agency client
            representation = context.make("Representation")
            representation.id = context.make_id("rep", company.id, principal.id)
            representation.add("agent", company)
            representation.add("client", principal)
            representation.add("startDate", p_registration_date)
            representation.add("endDate", p_termination_date)
            context.emit(representation)

        # Increment request count and wait
        request_count += 1
        if request_count >= max_entities_to_capture:
            context.log.info("Captured the maximum number of entities.")
            break  # Exit after capturing the first 5 entities

        # Wait for 15 seconds before the next request
        time.sleep(12)
