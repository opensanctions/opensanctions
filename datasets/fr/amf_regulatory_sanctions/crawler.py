from typing import Any, Generator, Optional, Tuple, Union
from zavod import Context
from datetime import datetime
import re
from html import unescape
from urllib.parse import urljoin

CLEAN_ENTITY = re.compile(r"<br />\r\n| et |;", re.IGNORECASE)
VALID_NAME_REGEX = re.compile(r"^[A-Za-z\s]+$")
BASE_URL = "https://www.amf-france.org"
# Add constants for specific keywords we want to omit from notes.
OMIT_KEYWORDS = [
    "Société",
    "société",
    "Banque",
    "la société",
    "Caisse",
    "cabinet",
    "La sociétés",
]

OMIT_TITLES = (
    "M.",
    "Mme.",
    "MME",
    "Madame",
    "MM.",
    "MM",
    "Monsieur",
    "de",
    "Mme",
    "de MM.",
    "de M.",
    "de Mmes",
    "Mlle",
    "M. ",
    "MM. ",
    "s",
)

CLEAN_ENTITY_REGEX = re.compile(
    r"\b(" + "|".join(OMIT_KEYWORDS) + r")\b", re.IGNORECASE
)

CLEAN_PERSON_REGEX = re.compile(
    r"\b(M\. |Mme\.|Mme |MM\.|MM\. |Madame|Monsieur )\b", re.IGNORECASE
)


def clean_name(name: str, clean_regex) -> str:
    # Unescape and clean the name
    unescaped_name = unescape(name)
    cleaned_name = re.sub(clean_regex, "", unescaped_name).strip()
    return print(cleaned_name)


def determine_gender(name: str) -> str:
    if name.startswith("Mme.") or name.startswith("Madame") or name.startswith("Mme"):
        return "female"
    elif name.startswith("M.") or name.startswith("MM.") or name.startswith("Monsieur"):
        return "male"
    return ""


def get_value(
    data: dict, keys: Tuple[str, ...], default: Optional[Any] = None
) -> Union[Optional[Any], Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


def is_valid_name(name: str) -> bool:
    # Define the regex for invalid single letter names or sequences
    invalid_single_letter_regex = re.compile(r"^[A-Z]$")

    # Split the names by comma and strip each part
    name_parts = [part.strip() for part in name.split(",")]  # split_comma_names(name)

    for part in name_parts:
        # Remove known prefixes if they exist
        for prefix in OMIT_TITLES:
            if part.startswith(prefix):
                part = part[len(prefix) :].strip()
        # print(part)

        # Check if part is "Société" followed by a single letter
        if (
            part == "Société"
            or part.startswith("Société")
            or part.startswith("société")
            or part.startswith("Banque")
            or part.startswith("la société")
            or part.startswith("Caisse")
            or part.startswith("cabinet")
            or part.startswith("La sociétés")
            and len(part.split()) == 2
            and part.split()[-1].isalpha()
            and len(part.split()[-1]) == 1
        ):
            return False

        # Check if the part is now a single letter
        if invalid_single_letter_regex.match(part):
            return False

    return True


def process_person(
    context: Context,
    title: str,
    listing_date: str,
    name: str,
    theme: str,
    link: str,
    item: dict,
):
    """Process a person entry."""
    person = context.make("Person")
    person.id = context.make_id(title, listing_date, name)

    # Determine gender based on title prefix
    gender = ""
    if name.startswith("Mme.") or name.startswith("Madame"):
        gender = "female"
    elif name.startswith("M.") or name.startswith("MM.") or name.startswith("Monsieur"):
        gender = "male"

    if gender:
        person.add("gender", gender)

    # Clean the name after determining the gender
    cleaned_name = clean_name(name, CLEAN_PERSON_REGEX)
    person.add("name", cleaned_name)

    # Set other fields for person
    person.add("notes", theme)
    person.add("notes", title)
    if link:
        full_link = urljoin(BASE_URL, link)
        person.add("sourceUrl", full_link)
    if "download" in item:
        download_links = item["download"].get("sanction", {}).get("links", {})
        if download_links:
            download_url = download_links.get("url")
            if download_url:
                full_download_url = urljoin(BASE_URL, download_url)
                person.add("sourceUrl", full_download_url)

    context.emit(person)


def process_entity(
    context: Context,
    title: str,
    listing_date: str,
    name: str,
    theme: str,
    link: str,
    item: dict,
):
    """Process a legal entity entry."""
    entity = context.make("LegalEntity")
    entity.id = context.make_id(title, listing_date, name)
    # Clean and add name
    cleaned_name = clean_name(name, CLEAN_ENTITY_REGEX)
    entity.add("name", cleaned_name)
    entity.add("notes", theme)
    entity.add("notes", title)
    if link:
        full_link = urljoin(BASE_URL, link)
        entity.add("sourceUrl", full_link)
    if "download" in item:
        download_links = item["download"].get("sanction", {}).get("links", {})
        if download_links:
            download_url = download_links.get("url")
            if download_url:
                full_download_url = urljoin(BASE_URL, download_url)
                entity.add("sourceUrl", full_download_url)
    context.emit(entity)


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
        entity_names = re.split(CLEAN_ENTITY, entities)
        link = item.get("link", {}).get("url", "")
        date_epoch = item.get("date")
        if date_epoch:
            listing_date = datetime.utcfromtimestamp(int(date_epoch)).strftime(
                "%Y-%m-%d"
            )
        else:
            listing_date = ""

        for name in entity_names:
            name = name.strip()  # Strip leading/trailing spaces
            if not name or not is_valid_name(name):
                continue

            # Handling specific Société cases
            if "Société" in name:
                process_entity(context, title, listing_date, name, theme, link, item)
            elif (
                name.startswith("M.")
                or name.startswith("Mme.")
                or name.startswith("Madame")
                or name.startswith("MM.")
            ):
                process_person(context, title, listing_date, name, theme, link, item)
            else:
                process_entity(context, title, listing_date, name, theme, link, item)
