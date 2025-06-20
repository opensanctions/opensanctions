import csv
from typing import Dict
from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    other_name = h.multi_split(row.pop("other name"), [",", ";"])
    birth_date_1_orig = row.pop("date of birth")
    birth_date_1 = h.extract_date(context.dataset, birth_date_1_orig or None)
    birth_place = row.pop("place of birth")
    nationality = row.pop("nationality")
    passport_number = row.pop("passport no.")
    position = row.pop("position")
    po_box = row.pop("postal code")
    fiscal_code = row.pop("fiscal code")
    phone_number = row.pop("phone number")
    address_1 = row.pop("address_1")
    address_2 = row.pop("address_2")
    city = row.pop("city")
    country = row.pop("country")
    addresses = []
    if ";" in address_1:
        addresses.extend(
            [h.make_address(context, full=a) for a in address_1.split(";")]
        )
    else:
        address = h.make_address(
            context,
            full=address_1,
            remarks=address_2,
            po_box=po_box,
            city=city,
            country=country,
        )
        addresses.append(address)
    entity_type = row.pop("type")

    if entity_type == "Person":
        entity = context.make("Person")
        entity.id = context.make_id(full_name, birth_date_1, birth_place)
        entity.add("name", full_name)
        entity.add("alias", other_name)
        entity.add("birthDate", birth_date_1, original_value=birth_date_1_orig)
        h.apply_date(entity, "birthDate", row.pop("date of birth 2", None))
        entity.add("birthPlace", birth_place)
        # Handle multiple nationalities
        entity.add("nationality", [n.strip() for n in nationality.split("/")])
        entity.add("passportNumber", passport_number)
        for address in addresses:
            h.copy_address(entity, address)
        entity.add("taxNumber", fiscal_code)
        entity.add("phone", phone_number)
        entity.add("position", position)
        entity.add("topics", "sanction")
        entity.add(
            "program",
            "Romania Government Decision No. 1.272/2005: List of Suspected Terrorists",
        )
        # Emit the entity
        context.emit(entity)
    elif entity_type == "Organization":
        entity = context.make("Organization")
        entity.id = context.make_id(full_name, po_box, address_1)
        entity.add("name", full_name)
        entity.add("alias", other_name)
        for address in addresses:
            h.copy_address(entity, address)
        entity.add("topics", "sanction")
        entity.add(
            "program",
            "Romania Government Decision No. 1.272/2005: List of Suspected Terrorists",
        )
        # Emit the entity
        context.emit(entity)
    else:
        context.log.warning("Unhandled entity type", type=entity_type)


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.url, cache_days=1)
    doc.make_links_absolute(context.dataset.url)
    url = doc.xpath(".//a[contains(text(), 'HG nr. 1.272/2005')]/@href")
    assert len(url) == 1, "Expected exactly one link in the document"
    h.assert_url_hash(context, url[0], "583e5e471beabb3b5bde7b259770998952bdfea0")
    # AL-ZINDANI, Shaykh Abd-al-Majid
    # ...
    # AL AKHTAR TRUST

    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
