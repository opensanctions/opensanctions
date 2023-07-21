import io
import csv
import time
from pathlib import Path
from typing import Optional, Dict, Any, Generator
from zipfile import ZipFile
from urllib.parse import urljoin
from pantomime.types import ZIP

from opensanctions.core import Context
from opensanctions import helpers as h

DOWNLOAD_URL = "https://sam.gov/api/prod/fileextractservices/v1/api/download/"


def parse_date(date: Optional[str]):
    if date in ("", "Indefinite", None):
        return None
    return h.parse_date(date, ["%Y-%m-%d", "%m/%d/%Y"])


def read_rows(zip_path: Path) -> Generator[Dict[str, Any], None, None]:
    with ZipFile(zip_path, "r") as zip:
        for file_name in zip.namelist():
            with zip.open(file_name) as zfh:
                fh = io.TextIOWrapper(zfh)
                reader = csv.DictReader(fh, delimiter=",", quotechar='"')
                for row in reader:
                    yield row


def crawl(context: Context) -> None:
    ms = str(int(time.time() * 1000))
    url = context.data_url.replace("RANDOM", ms)
    metadata = context.fetch_json(url)
    objects = metadata.pop("_embedded").pop("customS3ObjectSummaryList")
    data_url = urljoin(DOWNLOAD_URL, objects[0]["key"])
    path = context.fetch_resource("source.zip", data_url)
    context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
    for row in read_rows(path):
        classification = row.pop("Classification")
        schema = context.lookup_value("classifications", classification)
        if schema is None:
            context.log.warn("Unknown classification", classification=classification)
            continue
        entity = context.make(schema)
        sam_number = row.pop("SAM Number")
        entity.id = context.make_slug(sam_number)
        creation_date = parse_date(row.pop("Creation_Date", None))
        entity.add("createdAt", creation_date)
        entity.add("topics", "debarment")
        entity.add("notes", row.pop("Cross-Reference", None))
        entity.add_cast(
            "Ogranization",
            "registrationNumber",
            row.pop("Unique Entity ID", None),
        )

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

        entity.add("country", row.get("Country"))
        address = h.make_address(
            context,
            street=row.pop("Address 1", None),
            street2=row.pop("Address 2", None),
            # street3=row.pop("Address 3", None),
            city=row.pop("City", None),
            postal_code=row.pop("Zip Code", None),
            country=row.pop("Country", None),
            state=row.pop("State / Province", None),
        )
        if address is not None:
            entity.add("address", address.get("full"))
            entity.add("country", address.get("country"))
        # h.apply_address(context, entity, address)

        agency = row.pop("Excluding Agency")
        if agency == "TREAS-OFAC":
            # cf. us_ofac_sdn, us_ofac_cons
            continue
        sanction = h.make_sanction(context, entity, key=agency)
        if agency is not None and len(agency):
            sanction.set("authority", agency)
        sanction.add("authorityId", sam_number)
        sanction.add("program", row.pop("Exclusion Program"))
        sanction.add("provisions", row.pop("Exclusion Type"))
        sanction.add("listingDate", creation_date)
        sanction.add("startDate", parse_date(row.pop("Active Date")))
        sanction.add("endDate", parse_date(row.pop("Termination Date")))
        sanction.add("summary", row.pop("Additional Comments", None))

        # The NPI (National Provider Identifier) is a unique identification number
        # for covered health care providers. It is an optional field for exclusion
        # records.
        row.pop("NPI", None)

        row.pop("CT Code", None)

        # Commercial And Government Entity (CAGE) Code
        row.pop("CAGE", None)

        context.audit_data(row)
        context.emit(sanction)
        context.emit(entity, target=True)
    # print(data_url)
