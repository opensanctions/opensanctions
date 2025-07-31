from typing import Dict
from rigour.mime.types import PDF
import re

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource
from normality import squash_spaces

REGEX_AKA = re.compile(r"\baka\b|a\.k\.a\.?", re.IGNORECASE)
# Ruth Diane Jones, DO
# Tamika Brewer-Adams (PCA)
# Charles Michael Napoli (Pharma Tech)
REGEX_JOB_ROLE = re.compile(r"^(?P<name>.+)[,\s]+(?P<role>([A-Z\.,/-]+|\([^\)]+\)))$")


def crawl_item(row: Dict[str, str], context: Context):
    raw_name = squash_spaces(row.pop("sanctioned_provider_name"))
    npi = row.pop("npi")
    # Skip empty rows
    if raw_name == "" and npi == "":
        return
    names = REGEX_AKA.split(raw_name)
    name, alias = names[0], names[1:]
    position = None
    if match := REGEX_JOB_ROLE.match(name):
        if match.group("role").lower() not in {"llc", "inc", "corp", "pc"}:
            position = match.group("role")
            name = match.group("name").rstrip(",")
    dba = None
    if "d.b.a." in name:
        result = context.lookup("names", name)
        if result is not None:
            name, dba = result.values[0], result.values[1]
        else:
            context.log.warning("No lookups found for", name)

    entity = context.make("LegalEntity")
    entity.id = context.make_id(raw_name, npi)
    entity.add("name", name)
    entity.add("alias", dba)
    entity.add_cast("Person", "position", position)
    entity.add("alias", alias)
    entity.add("country", "us")
    entity.add("sector", row.pop("taxonomy"))
    dea = row.pop("dea")
    if dea != "-":
        entity.add("idNumber", dea)
    entity.add("npiCode", h.multi_split(npi, [";", ",", "&"]))

    # Deal with sometimes intentional line breaks, and sometimes unwanted wrapping:
    license_numbers = row.pop("license", row.pop("license_rn707737_pa", None))
    license_numbers = re.sub(
        r"PROMISe", ";PROMISe", license_numbers, flags=re.IGNORECASE
    )
    license_numbers = re.sub(
        r"PROMISe#?:?\n#?", "PROMISe# ", license_numbers, flags=re.IGNORECASE
    )
    for license_num in h.multi_split(license_numbers, [",", ";"]):
        if "\n" in license_num:
            res = context.lookup("license_numbers", license_num)
            if res:
                assert res.values, res
                entity.add("idNumber", res.values, original_value=license_num)
            else:
                context.log.warning(
                    "Unhandled license number", license_number=license_num
                )
        else:
            entity.add("idNumber", squash_spaces(license_num))

    sanction = h.make_sanction(context, entity)
    sanction.add("description", squash_spaces(row.pop("comments")))
    sanction.set("authority", squash_spaces(row.pop("oig_medicaid_sanction")))
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    reinstated_date = row.pop("reinstated_date")
    h.apply_date(sanction, "endDate", reinstated_date)

    if sanction.get("endDate"):
        is_debarred = h.is_active(sanction)
    else:
        res = context.lookup("type.date", reinstated_date)
        if res and res.is_debarred is not None:
            is_debarred = res.is_debarred
        else:
            is_debarred = True
            context.log.warn(
                "No `is_debarred` for end date mapping", reinstated=reinstated_date
            )

    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["year"])


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context, "source.pdf", context.data_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in h.parse_pdf_table(context, path, headers_per_page=True):
        crawl_item(item, context)
