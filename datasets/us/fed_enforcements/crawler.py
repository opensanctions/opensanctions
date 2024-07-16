from io import StringIO
import csv
from datetime import datetime
from urllib.parse import urljoin
import re
from followthemoney.types import registry

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
REGEX_CLEAN_COMMA = re.compile(
    r", \b(LLC|L\.L\.C|Inc|Jr|INC|L\.P|LP|Sr|III|II|IV|S\.A|LTD|USA INC|\(?A/K/A|\(?N\.K\.A|\(?N/K/A|\(?F\.K\.A|formerly known as|INCORPORATED)\b",  # noqa
    re.I,
)


def only_one_name(txt: str) -> tuple[str | None, str | None]:
    """Function to test if a given test is only the name of one bank. The names of the banks
    are structured as "[name of bank], [city], [state]". We are going to verify if the text matches
    this pattern and if the final part is indeed a US state, if it is, we can safely assume the text
    is the name of only one bank. Also, if the text doesn't have commas or and, we can safely assume
    it is the name of only one bank.

    Args:
        txt (str): The text to be tested

    Returns:
        tuple: The name, if it could be extracted, and the locality string, if it could also be extracted.
    """

    if "," not in txt and " and " not in txt:
        return (txt, None)

    # We are going to remove trailing spaces and commas to deal with cases such as "ABC, new york, new york,"
    txt = txt.strip().rstrip(",")

    matches = re.match(ONE_NAME_PATTERN, txt)

    if matches is None:
        return (None, None)

    is_us_state = matches.group(1).lower() in US_STATES_NAMES_AND_ACRONYMS
    is_country = (
        registry.country.clean(matches.group(1).lower(), fuzzy=True) is not None
    )

    if is_us_state or is_country:
        return txt.split(", ", 1)

    return (None, None)


def crawl_item(input_dict: dict, context: Context):
    if input_dict["Individual"]:
        schema = "Person"
        individual_name = input_dict.pop("Individual")

        names = [(individual_name, None)]
        result = context.lookup("individual_name", individual_name)
        if result:
            names = [(n, None) for n in result.values]
        elif len(individual_name) > 50:
            context.log.warn("Name too long", name=individual_name)
        affiliation = input_dict.pop("Individual Affiliation")
    else:
        schema = "Company"
        affiliation = None
        raw_name = input_dict.pop("Banking Organization")

        # We are going to remove the commas that are not useful (such as the ones that are before the INC, LLC, etc)
        raw_name = REGEX_CLEAN_COMMA.sub(r" \1", raw_name)

        # We can safely split the names by the semicolon, as the semicolon is used to separate the names of the banks
        banking_organizations = raw_name.split(";")
        names = []

        for banking_organization in banking_organizations:
            # If the name is only one name, we can safely assume it is the name of only one bank
            name, locality = only_one_name(banking_organization)
            if name:
                names.append((name, locality))
            # If the name has " and " and all the names are only one name, we can safely assume it is the name of multiple banks separeted by and
            elif " and " in banking_organization:
                orgs = banking_organization.split(" and ")
                split_orgs = [only_one_name(org) for org in orgs]
                if all(org[0] for org in split_orgs):
                    names.extend(split_orgs)
            # Else, we are going to split the names by the comma
            else:
                res = context.lookup("comma_names", banking_organization)
                if res:
                    names.extend([(n, None) for n in res.names])
                else:
                    context.log.warning(
                        "Not sure how to split on comma or and.",
                        text=banking_organization.lower(),
                    )
                    names.extend([(banking_organization, None)])

    effective_date = input_dict.pop("Effective Date")
    termination_date = input_dict.pop("Termination Date")
    url = input_dict.pop("URL")
    provisions = input_dict.pop("Action")
    sanction_description = input_dict.pop("Note")
    for name, locality in names:
        entity = context.make(schema)
        entity.id = context.make_id(name, affiliation, locality)
        entity.add("name", name)

        if locality:
            entity.add("address", locality)
            parts = locality.split(", ")
            if (
                parts[1].lower() not in US_STATES_NAMES_AND_ACRONYMS
            ) and registry.country.clean(parts[1], fuzzy=True):
                entity.add("country", parts[1])

        if schema == "Company":
            entity.add("topics", "fin.bank")

        sanction = h.make_sanction(context, entity, key=[effective_date])
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
            is_target = False
        # if it doesn't have a termination date, we assume the entity is still in the crime.fin topic
        else:
            entity.add("topics", "crime.fin")
            is_target = True

        context.emit(entity, target=is_target)
        context.emit(sanction)

    # Name = the string that appears in the url column
    context.audit_data(input_dict, ignore=["Name"])


def crawl(context: Context):
    response = context.fetch_text(context.data_url)

    for item in csv.DictReader(StringIO(response)):
        item["URL"] = urljoin(context.data_url, item["URL"])
        crawl_item(item, context)
