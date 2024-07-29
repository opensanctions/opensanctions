import requests
import time
from typing import Any, Generator, Optional, Tuple, Union
from zavod import Context


def get_value(
    data: dict, keys: Tuple[str, ...], default: Optional[Any] = None
) -> Union[Optional[Any], Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


def parse_json(context: Context) -> Generator[dict, None, None]:
    response = requests.get("https://efile.fara.gov/api/v1/Registrants/json/Active")

    # Check the response status
    if response.status_code == 200:
        data = response.json()

        # Check if the result contains data
        if isinstance(data, list) and data:
            context.log.info(f"Fetched {len(data)} results.")
            for item in data:
                yield item  # Yield each item in the result
        else:
            context.log.info("No data found.")

    elif response.status_code == 400:
        context.log.error(f"Bad request: {response.status_code}, {response.text}")

    elif response.status_code == 500:
        context.log.error(f"Server error: {response.status_code}, {response.text}")

    else:
        context.log.error(
            f"Unexpected status code: {response.status_code}, {response.text}"
        )


def crawl(context: Context) -> None:
    max_retries = 5
    current_retry = 0

    while current_retry < max_retries:
        try:
            # General data processing
            for item in parse_json(context):
                # Extract relevant fields from each item; modify keys to match the API response structure
                name = item.get("Name")
                address = item.get("Address_1")
                # city = item.get("City")
                # state = item.get("State")
                # zip_code = item.get("Zip")
                registration_date = item.get("Registration_Date")
                registration_number = item.get("Registration_Number")

                # Create a Company entity for each item
                company = context.make("Company")
                company.id = context.make_id(
                    name, address
                )  # Create a unique ID based on name and address
                company.add("name", name)
                company.add("address", address)
                # company.add("city", city)
                # company.add("state", state)
                # company.add("zip", zip_code)
                company.add("incorporationDate", registration_date)
                company.add("registrationNumber", registration_number)
                context.emit(company)
                
                # Increment request count and check if we need to wait
                request_count += 1
                
                if request_count >= max_requests:
                    elapsed_time = time.time() - start_time
                    
                    if elapsed_time < 10:
                        time_remaining = 10 - elapsed_time
                        context.log.info(f"Reached max requests, sleeping for {time_remaining} seconds.")
                        time.sleep(time_remaining)  # Wait to stay within the limit
                    # Reset the tracker
                    request_count = 0
                    start_time = time.time()  # Reset the time tracker