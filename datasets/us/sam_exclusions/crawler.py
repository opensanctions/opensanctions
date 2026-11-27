# How to review names in this dataset:
#
# - The standard rules in https://zavod.opensanctions.org/extract/names/#whats-a-clean-name apply
# - See the single_token_min_length settings in the metadata.
#   If it looks like a name was selected for review because it doesn't contain
#   a space (it's a single token) and is shorter than the threshold, follow the
#   link(s) to related entities.
#     - Acronyms or shortened forms of more complete names seen in the entity should generally be
#       added as alias unless they're really vague and likely to cause false matches.
#     - Non-person names that might match a person first/last name should usually be added as alias
#       rather than a primary name.

import io
import csv
import time
from datetime import date
from pathlib import Path
from typing import Any
from collections.abc import Generator
from zipfile import ZipFile
from urllib.parse import urljoin
from rigour.mime.types import ZIP
from rigour.text import is_nullword

from zavod import Context
from zavod import helpers as h

DOWNLOAD_URL = "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
IGNORE_COLUMNS = ["CT Code", "Open Data Flag"]


def parse_date(date: str | None) -> str | None:
    if date in ("", "Indefinite", None):
        return None
    return date


def clean_address_part(part: Any) -> str | None:
    if part is None:
        return None
    cleaned: str = str(part).strip()
    if (
        len(cleaned) == 0
        or cleaned in ("-", "XX")
        or is_nullword(cleaned, normalize=True)
    ):
        return None
    return cleaned


def read_rows(zip_path: Path) -> Generator[tuple[str, dict[str, str]], None, None]:
    with ZipFile(zip_path, "r") as zip:
        for file_name in zip.namelist():
            with zip.open(file_name) as zfh:
                fh = io.TextIOWrapper(zfh)
                reader = csv.DictReader(fh, delimiter=",", quotechar='"')
                for row in reader:
                    yield file_name, row


def crawl_data_url(context: Context) -> str:
    ms = str(int(time.time() * 1000))
    url = context.data_url.replace("RANDOM", ms)
    metadata = context.fetch_json(url)
    objects = metadata.pop("_embedded").pop("customS3ObjectSummaryList")
    for obj in objects:
        data_url: str = urljoin(DOWNLOAD_URL, obj["key"])
        if data_url.endswith(".ZIP"):
            return data_url
    raise RuntimeError("No ZIP file found")


def us_fed_excl_id(
    context: Context,
    full_name: str | None,
    first_name: str | None,
    middle_name: str | None,
    last_name: str | None,
    zip_code: str | None,
    city: str | None,
) -> str | None:
    id_name = h.make_name(
        full=full_name,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
    )
    id_zip = zip_code
    if id_zip is not None and len(id_zip) > 5:
        id_zip = id_zip[:5]
    return context.make_slug(
        id_name,
        id_zip,
        city,
        strict=False,
        prefix="us-fed-excl",
    )


def usgsa_id(
    context: Context,
    uei: str | None,
    full_name: str | None,
    first_name: str | None,
    middle_name: str | None,
    last_name: str | None,
    zip_code: str | None,
    city: str | None,
    country: str | None,
) -> str | None:
    id_name = h.make_name(
        full=full_name,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
    )
    id_zip = zip_code
    if id_zip is not None and len(id_zip) > 5:
        id_zip = id_zip[:5]
    return context.make_id(uei, id_name, id_zip, city, country)


def make_entity_id(context: Context, agency: str, row: dict[str, str]) -> str | None:
    if agency in ("HHS", "OPM"):
        return us_fed_excl_id(
            context,
            full_name=row.get("Name"),
            first_name=row.get("First"),
            middle_name=row.get("Middle"),
            last_name=row.get("Last"),
            zip_code=row.get("Zip Code"),
            city=row.get("City"),
        )
    return usgsa_id(
        context,
        uei=row.get("Unique Entity ID"),
        full_name=row.get("Name"),
        first_name=row.get("First"),
        middle_name=row.get("Middle"),
        last_name=row.get("Last"),
        zip_code=row.get("Zip Code"),
        city=row.get("City"),
        country=row.get("Country"),
    )


def find_listing_dates(context: Context, path: Path) -> dict[tuple[str, str], str]:
    """Find the earliest creation date of the rows behind each sanction.

    Creation_Date is reset when a record is edited, so of the rows that make up
    one sanction, the earliest comes closest to its listing.
    """
    dates: dict[tuple[str, str], str] = {}
    for _, row in read_rows(path):
        agency = row["Excluding Agency"]
        entity_id = make_entity_id(context, agency, row)
        created = parse_date(row["Creation_Date"])
        # Records migrated into SAM all carry the date of the migration.
        if entity_id is None or created is None or created == "2012-07-27":
            continue
        # Raises unless ISO, which the string comparison below needs.
        date.fromisoformat(created)
        key = (entity_id, agency)
        if key not in dates or created < dates[key]:
            dates[key] = created
    return dates


def sort_cross_ref(cross_ref: str) -> str:
    """
    Ensure ordering of cross-reference names is stable for values like
    `(also  Abbas ABDI ASJARD ,  Abbas ABDIASJERD ,  Abbas ABDI ESJERD )`
    """
    prefix = "(also  "
    suffix = " )"
    if not (cross_ref.startswith(prefix) and cross_ref.endswith(suffix)):
        return cross_ref
    inner = cross_ref[len(prefix) : -len(suffix)]
    names = sorted(n.strip() for n in inner.split(" , "))
    return prefix + " ,  ".join(names) + suffix


def crawl(context: Context) -> None:
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.zip", data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    listing_dates = find_listing_dates(context, path)
    schemata: dict[str, str] = {}
    for row_number, (filename, row) in enumerate(read_rows(path)):
        checksum = h.record_hash(row)
        classification = row.pop("Classification")
        schema = context.lookup_value("classifications", classification)
        if schema is None:
            context.log.warn("Unknown classification", classification=classification)
            continue
        agency = row.pop("Excluding Agency")
        entity_id = make_entity_id(context, agency, row)
        zip_code = row.pop("Zip Code")
        country = row.pop("Country")
        uei = row.pop("Unique Entity ID")

        if entity_id is None:
            context.log.warning(
                "No id for entity",
                name=row.get("Name"),
            )
            continue

        override_schema = context.lookup_value("schema.override", entity_id)
        schema = override_schema or schema
        entity = context.make(schema)
        entity.id = entity_id

        # Basically if the name is something like "Pete's sport and kickball",
        # run the crawler, unzip source.zip, check for rows where the same name can be
        # constructed from the various name columns, and check if the Classification
        # column differs for two rows that would generate the same id. Choose the
        # most appropriate schema and map the id to that schema in lookups.
        # Often a company is under both Firm and Special Designated Entity.
        # Prefer putting a sole trader like "John Smith d.b.a. John's business ltd"
        # under Company.
        if (
            not override_schema
            and entity.id in schemata
            and not entity.schema.is_a(schemata[entity.id])
        ):
            context.log.warning(
                "Schema mismatch",
                entity_id=entity.id,
                name=row.get("Name"),
                schema=entity.schema.name,
                prev_schema=schemata[entity.id],
                row=row_number,
            )
            continue
        schemata[entity.id] = entity.schema.name

        # All exclusions in this dataset are debarments from US federal programs
        # This previously used to tag entities as sanctioned when they are on US
        # OFAC lists, but this is problematic when GSA maintains exclusion records for
        # entities that have been delisted by OFAC.
        entity.add("topics", "debarment")

        cross_ref = row.pop("Cross-Reference")
        # if (
        #     cross_ref is not None
        #     and cross_ref.startswith("(also ")
        #     and cross_ref.endswith(")")
        # ):
        #     cross_ref = cross_ref[len("(also ") :].rstrip(")")
        #     cross_ref = cross_ref.replace(", LLC", " LLC")
        #     cross_ref = cross_ref.replace(", INC", " INC")
        #     cross_ref = cross_ref.replace(", JR", " JR")
        #     aliases = []
        #     for alias in cross_ref.split(", "):
        #         if len(alias) < 5 and len(aliases):
        #             aliases[-1] += ", " + alias
        #         else:
        #             aliases.append(alias)
        #     entity.add("alias", aliases, lang="eng")
        # else:
        cross_ref = sort_cross_ref(cross_ref)
        entity.add("notes", cross_ref, lang="eng")

        if "uniqueEntityId" in entity.schema.properties:
            entity.add("uniqueEntityId", uei)
        else:
            entity.add("registrationNumber", uei, quiet=True)

        entity.add("cageCode", row.pop("CAGE"), quiet=True)
        # The NPI (National Provider Identifier) is a unique identification number
        # for covered health care providers. It is an optional field for exclusion
        # records.
        npi = row.pop("NPI")
        if npi is not None and len(npi):
            if npi.endswith(".0"):
                npi = npi[:-2]
            entity.add("npiCode", npi)

        name = h.make_name(
            full=row.pop("Name"),
            first_name=row.get("First"),
            middle_name=row.get("Middle"),
            last_name=row.get("Last"),
            prefix=row.pop("Prefix"),
            suffix=row.pop("Suffix"),
        )

        if not name:
            context.log.warning("No name for entity", entity_id=entity.id)
            continue

        h.apply_reviewed_name_string(context, entity, string=name, lang="eng")
        entity.add("firstName", row.pop("First"), quiet=True, lang="eng")
        entity.add("middleName", row.pop("Middle"), quiet=True, lang="eng")
        entity.add("lastName", row.pop("Last"), quiet=True, lang="eng")

        state = clean_address_part(row.pop("State / Province"))
        entity.add("country", country)
        address = h.make_address(
            context,
            street=clean_address_part(row.pop("Address 1")),
            street2=clean_address_part(row.pop("Address 2")),
            # street3=row.pop("Address 3", None),
            city=clean_address_part(row.pop("City")),
            postal_code=clean_address_part(zip_code),
            country_code=entity.first("country"),
            state=state,
        )
        h.copy_address(entity, address)

        context.emit(entity)

        program = row.pop("Exclusion Program")
        provisions = row.pop("Exclusion Type")
        status = row.pop("Record Status")
        start_date = parse_date(row.pop("Active Date"))
        end_date = parse_date(row.pop("Termination Date"))
        summary = row.pop("Additional Comments")
        row.pop("Creation_Date")
        record_id = row.pop("SAM Number")

        # OFAC exclusions mirror the SDN list, whose sanctions us_ofac_sdn emits.
        if agency != "OFAC":
            sanction = h.make_sanction(context, entity, key=agency)
            if agency is not None and len(agency):
                sanction.set("authority", agency)

            sanction.add("program", program)
            sanction.add("provisions", provisions)
            sanction.add("status", status)
            listing_date = listing_dates.get((entity_id, agency))
            h.apply_date(sanction, "listingDate", listing_date)
            h.apply_date(sanction, "startDate", start_date)
            h.apply_date(sanction, "endDate", end_date)
            sanction.add("summary", summary)
            h.finalize_sanction(context, sanction, key=record_id, checksum=checksum)
            context.emit(sanction)

        context.audit_data(row, ignore=IGNORE_COLUMNS)
