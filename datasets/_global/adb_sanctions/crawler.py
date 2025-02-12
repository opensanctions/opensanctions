from typing import Dict
import re

from zavod import Context
from zavod import helpers as h

REG_NRS = ["(Reg. No:", "(Reg. No.:", "(Reg. No.", "(Trade Register No.:"]
ENTITY_SPLITS = [
    ";",
    "affiliates",
]
NAME_SPLITS = [
    "Previously known as",
    "also known as",
    "also doing business as",
    "formerly operating as",
    "also",
    "formerly",
    "f/k/a",
    "(AKA",
]
# MIRROR_URL = "https://data.opensanctions.org/contrib/adb_sanctions/data.html"
REGEX_ALIAS_REGNO = re.compile(
    r"(?P<name>.{5,30})[;,] (Registration no.|ID:) (?P<regno>.{5,20})", re.IGNORECASE
)
REGEX_INTERNAL_URL = re.compile(
    r"http://([\w-]+\.)+azurecontainerapps.io:80/api/published-list"
)


def crawl_row(context: Context, row: Dict[str, str | None]):
    full_name = row.pop("name") or ""

    # Split the full name using NAME_SPLITS first
    entities = h.multi_split(full_name, ENTITY_SPLITS)
    other_names = row.pop("otherName").replace("\\", "")
    country = row.pop("nationality") or ""
    country = country.replace("Non ADB Member Country", "")
    country = country.replace("Rep. of", "").strip()
    country = country.replace("*2", "").strip()

    grounds = row.pop("grounds")
    sanction_type = row.pop("sanctionType")
    addresses = row.pop("address").split(";")
    start_date = row.pop("effectiveDateOfSanction")
    end_date = row.pop("lapseDateOfSanction")
    modified_at = row.pop("changesMadeOn")

    for entity_names_str in entities:
        name_optional_regno = entity_names_str
        registration_number = None

        # Further split each part using REG_NRS
        for splitter in REG_NRS:
            if splitter in entity_names_str:
                entity_names_str, registration_number = entity_names_str.split(
                    splitter, 1
                )
                registration_number = registration_number.replace(")", "").strip()
                break

        entity = context.make("LegalEntity")
        entity.id = context.make_id(name_optional_regno, country)
        entity.add("name", h.multi_split(entity_names_str, NAME_SPLITS))

        if match := REGEX_ALIAS_REGNO.match(other_names):
            entity.add("alias", match.group("name"))
            entity.add("registrationNumber", match.group("regno"))
        elif ":" in other_names or "no." in other_names.lower():
            res = context.lookup("other_names", other_names)
            if res:
                for item in res.items:
                    entity.add(item["prop"], item["value"])
            else:
                context.log.warning("Unhandled other_names", value=other_names)
        else:
            entity.add("alias", other_names)

        entity.add("country", country)
        entity.add("registrationNumber", registration_number)
        entity.add("address", addresses)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", grounds)
        sanction.add("status", sanction_type)
        h.apply_date(sanction, "startDate", start_date)
        h.apply_date(sanction, "endDate", end_date)
        h.apply_date(sanction, "modifiedAt", modified_at)

        if h.is_active(sanction):
            entity.add("topics", "debarment")

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(row)


def crawl(context: Context):
    next_url = context.data_url + "?sortField=Name&isAscending=true"
    pages = 0
    while next_url:
        response = context.fetch_json(next_url)

        next_url = response["links"]["next"]
        if next_url is not None:
            next_url = REGEX_INTERNAL_URL.sub(context.data_url, next_url)

        for item in response["data"]:
            crawl_row(context, item["attributes"])

        pages += 1
        assert pages <= 500, "More pages than expected."
