from typing import Any, Generator, List, Optional, Tuple, Union
from zavod import Context
from datetime import datetime
import re
from html import unescape
from urllib.parse import urljoin
from pprint import pprint

from zavod.helpers.text import multi_split

CLEAN_ENTITY = re.compile(r"(<br />\r\n| et |;)", re.IGNORECASE)
BASE_URL = "https://www.amf-france.org"
OMIT_PREFIXES = [
    r"^Société$",
    r"^sociétés",
    r"^Société",
    r"^société",
    r"^Banque",
    r"^la société",
    r"^Caisse",
    r"^cabinet",
    r"^La sociétés",
    r"^les CABINETS",
    r"^les sociétés",
    r"^LA",
    r"^de gestion",
    r"^hui dénommée",
    r"^Melle",
    r"^cogérants de",
]

OMIT_PREFIXES_REGEX = re.compile("|".join(OMIT_PREFIXES), re.IGNORECASE)

# Add constants for specific keywords we want to omit from notes.
OMIT_KEYWORDS = [
    "Société",
    "société",
    "Banque",
    "la société",
    "Caisse",
    "cabinet",
    "La sociétés",
    "les CABINETS",
    "les sociétés",
    "LA",
    "de gestion",
    "hui dénommée",
    "Melle",
    "cogérants de",
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

female_prefixes = ["Mme", "Madame", "MME", "Mme.", "de Mmes", "Mlle"]
male_prefixes = [
    "M.",
    "MM.",
    "Monsieur",
    "M",
    "MM",
    "de MM.",
    "de M.",
    "M. ",
    "MM. ",
]

CLEAN_ENTITY_REGEX = re.compile(
    r"\b(" + "|".join(OMIT_KEYWORDS) + r")\b", re.IGNORECASE
)

CLEAN_PERSON_REGEX = re.compile(
    r"\b(M\. |Mme\.|Mme |MM\.|MM\. |Madame|Monsieur |Monsieur|Mme)\b", re.IGNORECASE
)

# SINGLE_LETTER_REGEX = re.compile(r"^[A-Z]$", re.IGNORECASE)
SINGLE_LETTER_REGEX = re.compile(r"^[A-Z]\s*$", re.IGNORECASE)

OMIT_KEYWORDS_REGEX = re.compile(
    r"(Sociétés?| Banque| Caisse| société| La sociétés| la société) [a-zA-Zé]\b"
)


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


def clean_name(name: str, clean_regex) -> str:
    # Unescape and clean the name
    unescaped_name = unescape(name)
    cleaned_name = re.sub(clean_regex, "", unescaped_name).strip()
    return cleaned_name


def determine_gender(name: str) -> str:
    for prefix in female_prefixes:
        if name.startswith(prefix):
            return "female"

    for prefix in male_prefixes:
        if name.startswith(prefix):
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


def roughly_valid_name(name: str) -> bool:
    if not re.search("\w{3}", name):
        return False
    return len(name) > 3


def is_valid_name(name: str) -> bool:
    # Split the names by comma and strip each part
    name_parts = [part.strip() for part in name.split(",")]  # split_comma_names(name)

    for part in name_parts:
        for prefix in OMIT_TITLES:
            if part.startswith(prefix):
                part = part.replace(prefix, "", 1)

        # print(part)
        # Check if part is "Société" followed by a single letter
        # if (
        #     part == "Société"
        #     or part.startswith("sociétés")
        #     or part.startswith("Société")
        #     or part.startswith("société")
        #     or part.startswith("sociétés")
        #     or part.startswith("Banque")
        #     or part.startswith("la société")
        #     or part.startswith("Caisse")
        #     or part.startswith("cabinet")
        #     or part.startswith("La sociétés")
        #     or part.startswith("les CABINETS")
        #     or part.startswith("les sociétés")
        #     or part.startswith("LA")
        #     or part.startswith("de gestion")
        #     or part.startswith("hui dénommée")
        #     or part.startswith("Melle")
        #     or part.startswith("cogérants de")
        #     and len(part.split()) == 2
        #     and part.split()[-1].isalpha()
        #     and len(part.split()[-1]) == 1
        # ):
        #     return False
        if OMIT_PREFIXES_REGEX.match(part):
            return False
        # Check if the part is now a single letter
        if SINGLE_LETTER_REGEX.match(part):
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

    gender = determine_gender(name)
    person.add("gender", gender)

    # Clean the name after determining the gender
    cleaned_name = clean_name(name, CLEAN_PERSON_REGEX)

    # Catch things that still don't look right
    if not roughly_valid_name(cleaned_name):
        name_fix_res = context.lookup("roughly_invalid_names", cleaned_name)
        if name_fix_res is None:
            context.log.warning("...")
            return
        cleaned_name = name_fix_res.value
    if cleaned_name is None:
        return
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

    context.emit(person, target=True)


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

    # Not warning if this one isn't defined, just an overall check
    precursor_res = context.lookup("name_fixes", name)
    if precursor_res:
        name = precursor_res.value

    cleaned_name = clean_name(name, CLEAN_ENTITY_REGEX)

    # Catch things that still don't look right
    if not roughly_valid_name(cleaned_name):
        name_fix_res = context.lookup("roughly_invalid_names", cleaned_name)
        if name_fix_res is None:
            context.log.warning("...")
            return
        cleaned_name = name_fix_res.value
    if cleaned_name is None:
        return

    print(cleaned_name)
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
    context.emit(entity, target=True)


def split_names(names_str: str) -> List[str]:
    names = multi_split(names_str, ["<br />","\r","\n","et",";"])
    return names


def clean_name(name: str) -> str | None:
    return name


def clean_names(names_str: str) -> List[str]:
    names = split_names(names_str)
    cleaned_names = []
    for name in names:
        cleaned_name = clean_name(name, CLEAN_ENTITY_REGEX)
        if cleaned_name:
            cleaned_names.append(cleaned_name)
    return cleaned_names


def crawl(context: Context) -> None:
    # General data
    for data in parse_json(context):
        # Extract relevant fields from each item
        theme = unescape(str(data.get("theme")))
        item = data.get("infos", {})
        title = item.get("title")
        names_str = item.get("text_egard", "")
        names = clean_names(names_str)
        print("\n", names_str)
        pprint(names)
        continue
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
            if any(keyword in name for keyword in OMIT_KEYWORDS):
                process_entity(context, title, listing_date, name, theme, link, item)
            elif any(prefix in name for prefix in female_prefixes):
                process_person(context, title, listing_date, name, theme, link, item)
            elif any(prefix in name for prefix in male_prefixes):
                process_person(context, title, listing_date, name, theme, link, item)
            else:
                process_entity(context, title, listing_date, name, theme, link, item)
