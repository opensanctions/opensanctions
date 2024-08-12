from typing import Any, Generator, List, Optional, Tuple, Union

from normality import collapse_spaces
from zavod import Context
from datetime import datetime
import re
from html import unescape
from lxml import html
from urllib.parse import urljoin
from pprint import pprint

from zavod.helpers.text import multi_split

CLEAN_ENTITY = re.compile(r"(<br />\r\n| et |;)", re.IGNORECASE)
BASE_URL = "https://www.amf-france.org"


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



def get_value(
    data: dict, keys: Tuple[str, ...], default: Optional[Any] = None
) -> Union[Optional[Any], Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default



# def process_person(
#    context: Context,
#    title: str,
#    listing_date: str,
#    name: str,
#    theme: str,
#    link: str,
#    item: dict,
# ):
#    """Process a person entry."""
#    person = context.make("Person")
#    person.id = context.make_id(title, listing_date, name)
#
#    gender = determine_gender(name)
#    person.add("gender", gender)
#
#    # Clean the name after determining the gender
#    cleaned_name = clean_name(name, CLEAN_PERSON_REGEX)
#
#    # Catch things that still don't look right
#    if not roughly_valid_name(cleaned_name):
#        name_fix_res = context.lookup("roughly_invalid_names", cleaned_name)
#        if name_fix_res is None:
#            context.log.warning("...")
#            return
#        cleaned_name = name_fix_res.value
#    if cleaned_name is None:
#        return
#    person.add("name", cleaned_name)
#
#    # Set other fields for person
#    person.add("notes", theme)
#    person.add("notes", title)
#    if link:
#        full_link = urljoin(BASE_URL, link)
#        person.add("sourceUrl", full_link)
#    if "download" in item:
#        download_links = item["download"].get("sanction", {}).get("links", {})
#        if download_links:
#            download_url = download_links.get("url")
#            if download_url:
#                full_download_url = urljoin(BASE_URL, download_url)
#                person.add("sourceUrl", full_download_url)
#
#    context.emit(person, target=True)


# def process_entity(
#    context: Context,
#    title: str,
#    listing_date: str,
#    name: str,
#    theme: str,
#    link: str,
#    item: dict,
# ):
#    """Process a legal entity entry."""
#    entity = context.make("LegalEntity")
#    entity.id = context.make_id(title, listing_date, name)
#    # Clean and add name
#
#    # Not warning if this one isn't defined, just an overall check
#    precursor_res = context.lookup("name_fixes", name)
#    if precursor_res:
#        name = precursor_res.value
#
#    cleaned_name = clean_name(name, CLEAN_ENTITY_REGEX)
#
#    # Catch things that still don't look right
#    if not roughly_valid_name(cleaned_name):
#        name_fix_res = context.lookup("roughly_invalid_names", cleaned_name)
#        if name_fix_res is None:
#            context.log.warning("...")
#            return
#        cleaned_name = name_fix_res.value
#    if cleaned_name is None:
#        return
#
#    print(cleaned_name)
#    entity.add("name", cleaned_name)
#    entity.add("notes", theme)
#    entity.add("notes", title)
#    if link:
#        full_link = urljoin(BASE_URL, link)
#        entity.add("sourceUrl", full_link)
#    if "download" in item:
#        download_links = item["download"].get("sanction", {}).get("links", {})
#        if download_links:
#            download_url = download_links.get("url")
#            if download_url:
#                full_download_url = urljoin(BASE_URL, download_url)
#                entity.add("sourceUrl", full_download_url)
#    context.emit(entity, target=True)

PREFIXES = [
    "Sociétés",
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
    "cabinets",
    "SCS",  # Société en commandite simple
    r"M\.?",
    "Mme",
    r"MM\.?",
    "Madame",
    "Monsieur",
    "Monsieur",
    "Melle",
]


comma_prefix_anon = "(,|et) +(" + "|".join(PREFIXES) + r")[\. \b]+[A-Z]\b"
print("comma_prefix_anon", comma_prefix_anon)
prefix_anon = "(" + "|".join(PREFIXES) + r")[\. \b]+[A-Z]\b"
print("prefix_anon", prefix_anon)
comma_anon =r"(,|et) +[A-Z]\b"
print("comma_anon", comma_anon)


def clean_names_str(names_str: str) -> str:
    names_str = re.sub(comma_prefix_anon, "", names_str)
    names_str = re.sub(prefix_anon, "", names_str)
    names_str = re.sub(comma_anon, "", names_str)
    names_str = collapse_spaces(names_str)
    return names_str


def crawl(context: Context) -> None:
    # General data
    for data in parse_json(context):
        #print("data", data)

        # Extract relevant fields from each item
        theme = unescape(str(data.get("theme")))
        item = data.get("infos", {})
        title = item.get("title")
        names_str = unescape(item.get("text_egard", ""))
        print("original", names_str)
        names_str = clean_names_str(names_str)
        print("cleaned", names_str)
        if not names_str:
            continue

        entities_res = context.lookup("entities", names_str)
        # No mapping yet
        if entities_res is None:
            context.log.warning("No entity mapping for %r" % names_str)
            continue

        # No entities for remaining names str
        if not entities_res.entities:
            continue

        penalty = item.pop("text")
        print("penalty", penalty)
        # html.fromstring(penalty)
        # Todo: figure out if we can assume cases like all/none exonorated and what topic we should give.
        # Perhaps we should just give all reg.action?

        for entity in entities_res.entities:
            # make entities here
            print("   ", entity)

        # Make it so you don't need a blank relations field in each entry
        if not entities_res.relationships:
            continue
        for rel in entities_res.relationships:
            # make relations here
            print("   ", rel)


        continue # just skip this stuff that should probably happen in the loop for now
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
    #print(SPLIT_NAMES_REGEX.pattern)
