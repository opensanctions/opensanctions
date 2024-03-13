from zavod import Context
import csv
from zavod import helpers as h
from typing import Dict, Any

FORMATS = ["%Y-%m-%d","%Y%m%d","%y-%m-%d"]


def get_address(row: Dict[str, Any]):
    address = row.get("ADDRESS")
    if address:
        address_data = address
    else:
        address_data = row.get("ZIP")  # Use ZIP code as ID for empty address
    return address_data

def crawl_item(context: Context, row: Dict[str, Any]):
    """Process data from CSV file."""
    if row["LASTNAME"].strip() or row["FIRSTNAME"].strip():
        entity = context.make("Person")
        firstname = row["FIRSTNAME"]
        midname = row["MIDNAME"]
        lastname = row["LASTNAME"]
        name = f"{firstname} {midname} {lastname}".strip()
        address_data = get_address(row)
        entity.id = context.make_slug(name, address_data)
        entity.add("birthDate", h.parse_date(row.get("DOB"), FORMATS))
        entity.add("name", name)
    else:
        entity = context.make("Company")
        name = row.get("BUSNAME")
        address_data = get_address(row)
        entity.id = context.make_slug(name, address_data)
        entity.add("name", name)
    entity.add("notes", row.get("GENERAL",None))
    entity.add("address", row.get("ADDRESS", None))
    entity.add("topics", "debarment")
    entity.add("country", "us")
    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", h.parse_date(row.get("EXCLDATE"), FORMATS))
    sanction.add("reason",row.get("EXCLTYPE"))
    sanction.add("program","US HHS OIG List of Excluded Individuals/Entities ")
    context.emit(sanction)
    context.emit(entity, target=True)
    context.audit_data(row,
                       ignore=[
                           "SPECIALTY",
                           "UPIN",
                           "NPI",
                           "CITY",
                           "STATE",
                           "REINDATE",
                           "WAIVERDATE",
                           "WVRSTATE"
                       ])
  

def crawl(context: Context) -> None:
    """Entry point for crawling process."""
    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            crawl_item(context, row)