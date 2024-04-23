from io import StringIO
import csv
from datetime import datetime
from urllib.parse import urljoin
import re

from zavod import Context, helpers as h

US_STATES_NAMES_AND_ACRONYMS = [
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "dc",
    "de",
    "fl",
    "ga",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "va",
    "wa",
    "wv",
    "wi",
    "wy",
]

ONE_NAME_PATTERN = r"^[^,]+, [^,]+, ([^,]+)$"


def is_only_one_name(txt: str) -> bool:
    """Function to test if a given test is only the name of one bank. The names of the banks
    are structured as "[name of bank], [city], [state]". We are going to verify if the text matches
    this pattern and if the final part is indeed a US state, if it is, we can safely assume the text
    is the name of only one bank.

    Args:
        txt (str): The text to be tested

    Returns:
        bool: If the txt is the name of only one bank.
    """

    matches = re.match(ONE_NAME_PATTERN, txt)

    if matches is not None and matches.group(1).lower() in US_STATES_NAMES_AND_ACRONYMS:
        return True

    return False


def crawl_item(input_dict: dict, context: Context):
    if input_dict["Individual"]:
        schema = "Person"
        names = [input_dict.pop("Individual")]
    else:
        schema = "Company"
        banking_organization = input_dict.pop("Banking Organization")

        if is_only_one_name(banking_organization):
            names = [banking_organization]
        elif " and " in banking_organization and all([is_only_one_name(name) for name in banking_organization.split(" and ")]):
            names = banking_organization.split(" and ")
        else:
            names = [
                name
                for possible_name in banking_organization.split(";")
                for name in h.split_comma_names(context, possible_name)
            ]

    effective_date = input_dict.pop("Effective Date")
    termination_date = input_dict.pop("Termination Date")
    url = input_dict.pop("URL")
    provisions = input_dict.pop("Action")
    sanction_description = input_dict.pop("Note")

    for name in names:
        entity = context.make(schema)
        entity.id = context.make_id(name)
        entity.add("name", name)

        if schema == "Company":
            entity.add("topics", "fin.bank")

        sanction = h.make_sanction(context, entity)
        sanction.add("startDate", h.parse_date(effective_date, formats=["%Y-%m-%d"]))
        sanction.add("provisions", provisions)
        sanction.add("description", sanction_description)

        if url != "DNE":
            sanction.add("sourceUrl", url)

        if termination_date != "":
            # if the termination date, is in the future, we assume the entity is still in the crime.fin topic
            if termination_date > datetime.today().strftime("%Y-%m-%d"):
                entity.add("topics", "crime.fin")

            sanction.add(
                "endDate", h.parse_date(termination_date, formats=["%Y-%m-%d"])
            )
        # if it doesn't have a termination date, we assume the entity is still in the crime.fin topic
        else:
            entity.add("topics", "crime.fin")

        context.emit(entity, target=True)
        context.emit(sanction)

    # Individual Affiliation = The bank of the individual
    # Name = the string that appears in the url column
    context.audit_data(input_dict, ignore=["Individual Affiliation", "Name"])


def crawl(context: Context):
    response = context.fetch_text(context.data_url)

    for item in csv.DictReader(StringIO(response)):
        item["URL"] = urljoin(context.data_url, item["URL"])
        crawl_item(item, context)
