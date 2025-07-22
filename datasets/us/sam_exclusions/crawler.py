import io
import csv
import time
from pathlib import Path
from typing import Optional, Dict, Any, Generator
from zipfile import ZipFile
from urllib.parse import urljoin
from rigour.mime.types import ZIP

from zavod import Context
from zavod import helpers as h

DOWNLOAD_URL = "https://sam.gov/api/prod/fileextractservices/v1/api/download/"


def parse_date(date: Optional[str]):
    if date in ("", "Indefinite", None):
        return None
    return date


def read_rows(zip_path: Path) -> Generator[Dict[str, Any], None, None]:
    with ZipFile(zip_path, "r") as zip:
        for file_name in zip.namelist():
            with zip.open(file_name) as zfh:
                fh = io.TextIOWrapper(zfh)
                reader = csv.DictReader(fh, delimiter=",", quotechar='"')
                for row in reader:
                    yield row


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


def crawl(context: Context) -> None:
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.zip", data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    schemata: Dict[str, str] = {}
    for row in read_rows(path):
        classification = row.pop("Classification")
        schema = context.lookup_value("classifications", classification)
        if schema is None:
            context.log.warn("Unknown classification", classification=classification)
            continue
        agency = row.pop("Excluding Agency")
        sam_number = row.pop("SAM Number")
        override_schema = context.lookup_value("schema.override", sam_number)
        schema = override_schema or schema
        entity = context.make(schema)
        zip_code = row.pop("Zip Code", None)
        entity.id = context.make_slug(sam_number)
        if agency in ("HHS", "OPM"):
            id_name = h.make_name(
                full=row.get("Name"),
                first_name=row.get("First"),
                middle_name=row.get("Middle"),
                last_name=row.get("Last"),
            )
            id_zip = zip_code
            if id_zip is not None and len(id_zip) > 5:
                id_zip = id_zip[:5]
            entity.id = context.make_slug(
                id_name,
                id_zip,
                row.get("City"),
                strict=False,
                prefix="us-fed-excl",
            )

        if entity.id is None:
            context.log.warning(
                "No id for entity",
                sam_number=sam_number,
                name=row.get("Name"),
            )
            continue

        if entity.id in schemata and not entity.schema.is_a(schemata[entity.id]):
            context.log.warning(
                "Schema mismatch",
                entity_id=entity.id,
                sam_number=sam_number,
                name=row.get("Name"),
                schema=entity.schema.name,
                prev_schema=schemata[entity.id],
            )
            continue
        schemata[entity.id] = entity.schema.name

        creation_date = parse_date(row.pop("Creation_Date", None))
        h.apply_date(entity, "createdAt", creation_date)
        if agency == "TREAS-OFAC":
            entity.add("topics", "sanction")
        else:
            entity.add("topics", "debarment")
        cross_ref = row.pop("Cross-Reference", None)
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
        uei = row.pop("Unique Entity ID", None)
        if entity.schema.is_a("LegalEntity"):
            entity.add("uniqueEntityId", uei)
        else:
            entity.add("registrationNumber", uei, quiet=True)

        entity.add("cageCode", row.pop("CAGE", None), quiet=True)
        # The NPI (National Provider Identifier) is a unique identification number
        # for covered health care providers. It is an optional field for exclusion
        # records.
        npi = row.pop("NPI", None)
        if npi is not None and len(npi):
            entity.add("npiCode", npi)

        h.apply_name(
            entity,
            full=row.pop("Name", None),
            first_name=row.pop("First", None),
            middle_name=row.pop("Middle", None),
            last_name=row.pop("Last", None),
            prefix=row.pop("Prefix", None),
            suffix=row.pop("Suffix", None),
            lang="eng",
            quiet=True,
        )

        state = row.pop("State / Province", None)
        country = row.pop("Country", None)
        address = h.make_address(
            context,
            street=row.pop("Address 1", None),
            street2=row.pop("Address 2", None),
            # street3=row.pop("Address 3", None),
            city=row.pop("City", None),
            postal_code=zip_code,
            country=country,
            state=state,
        )
        h.copy_address(entity, address)
        # h.apply_address(context, entity, address)

        sanction = h.make_sanction(context, entity, key=agency)
        if agency is not None and len(agency):
            sanction.set("authority", agency)
            # entity.set("program", agency)
        sanction.add("authorityId", sam_number)
        sanction.add("program", row.pop("Exclusion Program"))
        sanction.add("provisions", row.pop("Exclusion Type"))
        h.apply_date(sanction, "listingDate", creation_date)
        h.apply_date(sanction, "startDate", parse_date(row.pop("Active Date")))
        h.apply_date(sanction, "endDate", parse_date(row.pop("Termination Date")))
        sanction.add("summary", row.pop("Additional Comments", None))

        context.audit_data(
            row,
            ignore=["CT Code", "Open Data Flag"],
        )
        context.emit(sanction)
        context.emit(entity)
    # print(data_url)
