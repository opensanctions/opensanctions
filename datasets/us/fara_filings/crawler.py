import io
from datetime import datetime
from lxml import etree
from rigour.mime.types import ZIP
from pathlib import Path
from typing import Any, Dict, Generator
from zipfile import ZipFile

from zavod import Context, helpers as h

REGISTRANTS_URL = "https://efile.fara.gov/bulk/zip/FARA_All_Registrants.xml.zip"
REGISTRANTS_NAME = "FARA_All_Registrants.xml"
PRINCIPALS_URL = "https://efile.fara.gov/bulk/zip/FARA_All_ForeignPrincipals.xml.zip"
PRINCIPALS_NAME = "FARA_All_ForeignPrincipals.xml"


def read_rows(
    context: Context, zip_path: Path, file_name
) -> Generator[Dict[str, Any], None, None]:
    with ZipFile(zip_path, "r") as zip:
        with zip.open(file_name) as zfh:
            fh = io.TextIOWrapper(zfh, encoding="iso-8859-1")
            doc = etree.parse(fh)
            for node in doc.findall(".//ROW"):
                yield {el.tag: el.text for el in node}


def registrant_id(context: Context, registration_number: str) -> str:
    return context.make_slug("reg", registration_number)


def crawl_registrant(context: Context, item: Dict[str, Any]) -> None:
    # Extract relevant fields from each item
    address = h.make_address(
        context,
        street=item.pop("Address_1", None),
        street2=item.pop("Address_2", None),
        city=item.pop("City", None),
        postal_code=item.pop("Zip", None),
        state=item.pop("State", None),
    )
    termination_date = h.extract_date(
        context.dataset, item.pop("Termination_Date", None)
    )
    registration_number = item.pop("Registration_Number")

    entity = context.make("LegalEntity")
    entity.id = registrant_id(context, registration_number)
    entity.add("name", item.pop("Name").strip() or None)
    entity.add("topics", "role.lobby")
    h.apply_address(context, entity, address)
    entity.add("registrationNumber", registration_number)
    h.apply_date(entity, "createdAt", item.pop("Registration_Date"))
    entity.add("country", "us")
    if termination_date:
        entity.add("description", f"Terminated registration {termination_date[0]}")
    context.emit(entity)

    business_name = item.pop("Business_Name", "").strip()
    if business_name:
        business_entity = context.make("LegalEntity")
        business_entity.id = context.make_slug(
            "reg", registration_number, "biz", business_name
        )
        business_entity.add("name", business_name)
        business_entity.add("country", "us")

        context.emit(business_entity)
        link = context.make("UnknownLink")
        link.id = context.make_slug("link", business_entity.id)
        link.add("subject", entity)
        link.add("object", business_entity)

        context.emit(business_entity)
        context.emit(link)

    context.audit_data(item)


def crawl_principal(context: Context, item: Dict[str, Any]) -> None:
    # Add relevant agency client information to the company entity
    p_name = item.pop("Foreign_principal")
    address = h.make_address(
        context,
        street=item.pop("Address_1", None),
        street2=item.pop("Address_2", None),
        city=item.pop("City", None),
        postal_code=item.pop("Zip", None),
        state=item.pop("State", None),
    )
    registration_number = item.pop("Registration_number")

    # Now create a new Company entity for the agency client
    principal = context.make("LegalEntity")
    full_address = address.get("full") if address else None
    principal.id = context.make_id("principal", p_name, full_address)
    principal.add("name", p_name)
    principal.add("country", item.pop("Country_location_represented"))
    h.apply_address(context, principal, address)

    # Emit the new agency client entity
    context.emit(principal)
    # Create a relationship between the company and the agency client
    representation = context.make("Representation")
    representation.id = context.make_id("rep", registration_number, principal.id)
    representation.add("agent", registrant_id(context, registration_number))
    representation.add("client", principal)

    h.apply_date(representation, "startDate", item.pop("FP_registration_date"))
    h.apply_date(representation, "endDate", item.pop("FP_termination_date", None))

    context.audit_data(
        item,
        [
            "Registrant_name",
            "Registration_date",
        ],
    )
    context.emit(representation)


def crawl(context: Context) -> None:
    registrants_path = context.fetch_resource(
        REGISTRANTS_NAME,
        REGISTRANTS_URL + "?cachebust=" + datetime.now().isoformat(),
    )
    context.export_resource(registrants_path, ZIP, title=context.SOURCE_TITLE)
    for item in read_rows(context, registrants_path, REGISTRANTS_NAME):
        crawl_registrant(context, item)

    principals_path = context.fetch_resource(
        PRINCIPALS_NAME,
        PRINCIPALS_URL + "?cachebust=" + datetime.now().isoformat(),
    )
    context.export_resource(principals_path, ZIP, title=context.SOURCE_TITLE)
    for item in read_rows(context, principals_path, PRINCIPALS_NAME):
        crawl_principal(context, item)
