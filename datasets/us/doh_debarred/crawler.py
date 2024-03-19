from zavod import Context
import csv
from zavod import helpers as h
from typing import Dict, Any

FORMATS = ["%Y%m%d"]
def get_first_line(row: Dict[str, Any]):
    address = row.pop("ADDRESS")
    if address:
        address_data = address
    else:
        address_data = row.get("ZIP")  # Use ZIP code as ID for empty address
    return address_data

def crawl_item(context: Context, row: Dict[str, Any]):
    if row["LASTNAME"].strip() or row["FIRSTNAME"].strip():
        entity = context.make("Person")
        full_name=h.make_name(
        first_name = row.pop("FIRSTNAME",None),
        middle_name = row.pop("MIDNAME",None),
        last_name = row.pop("LASTNAME",None)
        )
        
        context.log.info(full_name)
        address_data = get_first_line(row)
        entity.id = context.make_slug(full_name, address_data)
        h.apply_name(entity=entity,full=full_name,lang='eng')
        entity.add("birthDate", h.parse_date(row.pop("DOB",None), FORMATS))
        entity.add("position",f'{row.pop("GENERAL",None)} {row.pop("SPECIALTY",None)}')
    else:
        entity = context.make("Company")
        name = row.pop("BUSNAME",None)
        address_data = get_first_line(row)
        entity.id = context.make_slug(name, address_data)
        entity.add("name", name)
        entity.add("description",f'{row.pop("GENERAL",None)} {row.pop("SPECIALTY",None)}')
   
    address=h.make_address(
        context,
        city=row.pop("CITY",None),
        state=row.pop("STATE",None),
        postal_code=row.pop("ZIP",None),
        country_code="us"
    )
    h.apply_address(context,entity,address)
    h.make_identification(
        context,
        entity,
        key=row.pop("UPIN",None),
        number=row.pop("NPI",None),
    )
    entity.add("topics", "debarment")
    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", h.parse_date(row.pop("EXCLDATE",None), FORMATS))
    sanction.add("reason",row.pop("EXCLTYPE",None))
    sanction.add("program","US HHS OIG List of Excluded Individuals/Entities ")
    waiver_start=row.pop("WAIVERDATE",None)
    if waiver_start!=0:
        waiver_state=row.pop("WVRSTATE",None)
        sanction.add("provisions",f'{h.parse_date(waiver_start,FORMATS)},{waiver_state}')
    context.emit(sanction)
    context.emit(entity, target=True)
    context.audit_data(row,ignore=["REINDATE" ])
  

def crawl(context: Context) -> None:
    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            crawl_item(context, row)