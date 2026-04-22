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
from pathlib import Path
from typing import Literal, Optional, Dict, Any, Generator, Tuple
from zipfile import ZipFile
from urllib.parse import urljoin
from pydantic import BaseModel
from rigour.mime.types import ZIP

from zavod import Context
from zavod import helpers as h


DOWNLOAD_URL = "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
IGNORE_COLUMNS = ["CT Code", "Open Data Flag", "SAM Number"]

NameProp = Literal["name", "alias", "weakAlias"]


class FullName(BaseModel):
    name: str
    property_name: NameProp


def parse_date(date: Optional[str]):
    if date in ("", "Indefinite", None):
        return None
    return date


def clean_address_part(part: Any) -> Optional[str]:
    if part is None:
        return None
    part = str(part).strip()
    if len(part) == 0 or part == "-" or part == "XX":
        return None
    return part


def read_rows(zip_path: Path) -> Generator[Tuple[str, Dict[str, str]], None, None]:
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
        data_url = urljoin(DOWNLOAD_URL, obj["key"])
        if data_url.endswith(".ZIP"):
            return data_url
    raise RuntimeError("No ZIP file found")


def us_fed_excl_id(
    context: Context,
    full_name: Optional[str],
    first_name: Optional[str],
    middle_name: Optional[str],
    last_name: Optional[str],
    zip_code: Optional[str],
    city: Optional[str],
) -> Optional[str]:
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
    uei: Optional[str],
    full_name: Optional[str],
    first_name: Optional[str],
    middle_name: Optional[str],
    last_name: Optional[str],
    zip_code: Optional[str],
    city: Optional[str],
    country: Optional[str],
) -> Optional[str]:
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


def crawl(context: Context) -> None:
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.zip", data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    schemata: Dict[str, str] = {}
    for row_number, (filename, row) in enumerate(read_rows(path)):
        classification = row.pop("Classification")
        schema = context.lookup_value("classifications", classification)
        if schema is None:
            context.log.warn("Unknown classification", classification=classification)
            continue
        agency = row.pop("Excluding Agency")

        zip_code = row.pop("Zip Code")
        country = row.pop("Country")
        uei = row.pop("Unique Entity ID")

        if agency in ("HHS", "OPM"):
            entity_id = us_fed_excl_id(
                context,
                full_name=row.get("Name"),
                first_name=row.get("First"),
                middle_name=row.get("Middle"),
                last_name=row.get("Last"),
                zip_code=zip_code,
                city=row.get("City"),
            )
        else:
            entity_id = usgsa_id(
                context,
                uei=uei,
                full_name=row.get("Name"),
                first_name=row.get("First"),
                middle_name=row.get("Middle"),
                last_name=row.get("Last"),
                zip_code=zip_code,
                city=row.get("City"),
                country=country,
            )

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

        creation_date = parse_date(row.pop("Creation_Date"))
        h.apply_date(entity, "createdAt", creation_date)

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
            return
        full_name_prop: NameProp = "name"
        # Not vessels
        if len(name) < 5 and entity.schema.is_a("LegalEntity"):
            full_name_prop = "weakAlias"
        elif len(name) < 10 and " " not in name and entity.schema.is_a("Person"):
            full_name_prop = "weakAlias"
        # Treat longer single word entity names as iffy for now
        # len("Sebastiano") == 10
        elif len(name) < 11 and " " not in name and entity.schema.is_a("LegalEntity"):
            full_name_prop = "alias"

        extraction = FullName(name=name, property_name=full_name_prop)
        origin = filename

        entity.add(
            extraction.property_name,
            extraction.name,
            lang="eng",
            origin=origin,
        )

        # The low quality names tend to come from OFAC so check those.
        if agency == "TREAS-OFAC":
            original = h.Names(name=name)
            is_irregular, suggested = h.check_names_regularity(entity, original)

            # A review will be created if standard heuristics suggest the name is irregular,
            # or if there is a custom suggestion that differs from the original categorisation.
            h.review_names(
                context,
                entity,
                original=original,
                suggested=suggested,
                is_irregular=is_irregular,
            )

        # TODO: Once we're done with reviews and change the OFAC clause to apply_reviewed_names,
        # and remove the heuristic-based cleaning/adding above, add the rest normally:
        # else:
        #     entity.add("name", name, lang="eng")

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

        sanction = h.make_sanction(context, entity, key=agency)
        if agency is not None and len(agency):
            sanction.set("authority", agency)

        sanction.add("program", row.pop("Exclusion Program"))
        sanction.add("provisions", row.pop("Exclusion Type"))
        sanction.add("status", row.pop("Record Status"))
        h.apply_date(sanction, "listingDate", creation_date)
        h.apply_date(sanction, "startDate", parse_date(row.pop("Active Date")))
        h.apply_date(sanction, "endDate", parse_date(row.pop("Termination Date")))
        sanction.add("summary", row.pop("Additional Comments"))
        context.emit(sanction)

        context.audit_data(row, ignore=IGNORE_COLUMNS)
