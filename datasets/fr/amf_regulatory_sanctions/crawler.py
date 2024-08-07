from typing import Any, Generator, Optional, Tuple, Union
from zavod import Context
from datetime import datetime
import re
from html import unescape
from urllib.parse import urljoin

CLEAN_ENTITY = re.compile(r"<br />\r\n| et |;", re.IGNORECASE)
CLEAN_NAME = re.compile(r"^M\. |^Mme\.|^MM\.|^Madame ", re.IGNORECASE)

BASE_URL = "https://www.amf-france.org"


def get_value(
    data: dict, keys: Tuple[str, ...], default: Optional[Any] = None
) -> Union[Optional[Any], Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


def parse_json(context: Context) -> Generator[dict, None, None]:
    response = context.fetch_json(context.data_url)
    if "data" not in response:
        context.log.info("No data available.")
        return

    context.log.info(
        f"Fetched {len(response['data'])} results."
    )  # Log the number of results
    for item in response["data"]:
        yield item


def crawl(context: Context) -> None:
    # General data
    for data in parse_json(context):
        # Extract relevant fields from each item
        theme = unescape(str(data.get("theme")))
        item = data.get("infos", {})
        title = item.get("title")
        entities = item.get("text_egard", "")
        # entity_names = re.split(r"<br />\r\n| et |;", entities)
        entity_names = re.split(CLEAN_ENTITY, entities)
        # total_amount = re.sub(
        #     r"[^\d]", "", item.get("text", "")
        # )  # Extract the numerical value only
        link = item.get("link", {}).get("url", "")
        date_epoch = item.get("date")
        # appeal = item.get("recours")

        if date_epoch:
            listing_date = datetime.utcfromtimestamp(int(date_epoch)).strftime(
                "%Y-%m-%d"
            )
        else:
            listing_date = ""

        for name in entity_names:
            name = name.strip()  # Strip leading/trailing spaces
            if not name:
                continue

            if (
                name.startswith("M.")
                or name.startswith("Mme.")
                or name.startswith("Madame")
                or name.startswith("MM.")
            ):
                person = context.make("Person")
                person.id = context.make_id(title, listing_date, name)
                # Remove M. or Mme. before adding the person's name
                cleaned_name = re.sub(CLEAN_NAME, "", name).strip()
                person.add("name", cleaned_name)
                if name.startswith("Mme." or "Madame"):
                    person.add("gender", "female")
                elif name.startswith("M." or "MM."):
                    person.add("gender", "male")
                # Set other fields for person
                person.add("notes", theme)
                person.add("notes", title)
                if link:
                    full_link = urljoin(BASE_URL, link)
                    person.add("sourceUrl", full_link)
                if "download" in item:
                    download_links = (
                        item["download"].get("sanction", {}).get("links", {})
                    )
                    if download_links:
                        download_url = download_links.get("url")
                        if download_url:
                            full_download_url = urljoin(BASE_URL, download_url)
                            person.add("sourceUrl", full_download_url)

                context.emit(person)

            else:
                entity = context.make("LegalEntity")
                entity.id = context.make_id(title, listing_date, name)
                entity.add("name", name)
                entity.add("notes", theme)
                entity.add("notes", title)
                if link:
                    full_link = urljoin(BASE_URL, link)
                    entity.add("sourceUrl", full_link)
                if "download" in item:
                    download_links = (
                        item["download"].get("sanction", {}).get("links", {})
                    )
                    if download_links:
                        download_url = download_links.get("url")
                        if download_url:
                            full_download_url = urljoin(BASE_URL, download_url)
                            entity.add("sourceUrl", full_download_url)

                context.emit(entity)
