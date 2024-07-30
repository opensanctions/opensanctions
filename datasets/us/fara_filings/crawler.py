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
        # Use fetch_json to get the response
        data = context.fetch_json(link, cache_days=1)

        # Check if data is a dictionary
        if isinstance(data, dict):
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
        else:
            context.log.error("Response data is not in the expected dictionary format.")

        # Error handling based on expected status codes
        if data is None:  # Handle cases where fetch_json might return None
            context.log.error("Failed to fetch data, received None.")
        elif isinstance(data, dict):
            if data.get("status_code") == 400:
                context.log.error(
                    f"Bad request: {data['status_code']}, {data.get('text')}"
                )
            elif data.get("status_code") == 429:
                context.log.error(
                    f"Too Many Requests: {data['status_code']}, {data.get('text')}"
                )
                time.sleep(10)  # Wait for the window to reset
            elif data.get("status_code") == 500:
                context.log.error(
                    f"Server error: {data['status_code']}, {data.get('text')}"
                )
            else:
                context.log.error(
                    f"Unexpected status code: {data.get('status_code')}, {data.get('text')}"
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
        data = context.fetch_json(
            agency_url, cache_days=1
        )  # Use fetch_json to get the response

        # ** Check if data is a dictionary **
        if isinstance(data, dict):
            # ** Check for Active and Terminated Foreign Principals **
            if "FOREIGNPRINCIPALS_ACTIVE" in data:
                if isinstance(data["FOREIGNPRINCIPALS_ACTIVE"]["ROW"], list):
                    return data["FOREIGNPRINCIPALS_ACTIVE"]["ROW"][
                        0
                    ]  # Return the first item if it's a list
                return data["FOREIGNPRINCIPALS_ACTIVE"]["ROW"]  # Return the dictionary

            elif "FOREIGNPRINCIPALS_TERMINATED" in data:
                if isinstance(data["FOREIGNPRINCIPALS_TERMINATED"]["ROW"], list):
                    return data["FOREIGNPRINCIPALS_TERMINATED"]["ROW"][
                        0
                    ]  # Return the first item if it's a list
                return data["FOREIGNPRINCIPALS_TERMINATED"][
                    "ROW"
                ]  # Return the dictionary

            else:
                context.log.info(
                    f"No agency client found for registration number {registration_number}."
                )
                return None

        # ** Handle cases where fetch_json might return None or not a dictionary **
        if data is None:
            context.log.error(
                f"Failed to fetch data for agency client using URL: {agency_url}. Received None."
            )
        else:
            context.log.error(
                f"Response is not in expected dictionary format for agency client."
            )

    return None


def crawl(context: Context) -> None:
    max_entities_to_capture = 1  # Limit to the first entity
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
