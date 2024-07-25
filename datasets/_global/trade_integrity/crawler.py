import requests
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
    page = 1  # Start from the first page
    while True:
        response = requests.get(
            f"https://trade-integrity.org/api/v1/search/?q=$&limit=100&page={page}"
        )

        if response.status_code != 200:
            context.log.error(
                f"Failed to fetch data: {response.status_code}, {response.text}"
            )
            break  # Exit the loop on error

        data = response.json()

        # Check for the "result" key in the response
        if "result" not in data or not data["result"]:
            context.log.info("No more data available.")
            break  # Exit the loop if no results are found

        context.log.info(
            f"Fetched {len(data['result'])} results from page {page}."
        )  # Log the number of results

        for item in data["result"]:
            yield item  # Yield each item

        page += 1  # Increment to the next page


def crawl(context: Context) -> None:
    # General data
    for item in parse_json(context):
        # Extract relevant fields from each item
        company_name = item.get("companyName")
        address = item.get("addressString")
        country_code = item.get("countryCode")
        amount_of_transactions = item.get("amountOfTransactionsUSD")
        number_of_transactions = item.get("numberOfTransactions")
        latitude = item.get("latitude")
        longitude = item.get("longitude")

        # Create a Company entity for each item
        company = context.make("Company")
        company.id = context.make_id(company_name, address)
        company.add("address", address)
        company.add("country", country_code)
        company.add("amountUsd", amount_of_transactions)
        company.add("notes", number_of_transactions)
        company.add("address", latitude)
        company.add("address", longitude)

        context.emit(company)
