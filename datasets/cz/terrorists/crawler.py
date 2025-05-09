from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html

PROGRAM = "Designated Terrorist Entities under Czech Government Regulation 210/2008"
# Announced 17 June 2008, date of effect 17 June 2008
START_DATE = "2008-06-17"


def crawl_details(context: Context, details: str) -> None:
    result = context.lookup("details", details)
    if not result or not result.details:
        context.log.warning("Details are not parsed", details=details)
        return

    override = result.details[0]

    entity = context.make(override.get("schema"))
    if entity.schema.is_a("Organization"):
        name_org = override.get("name")
        entity.id = context.make_id(name_org)
        entity.add("name", name_org)
        entity.add("alias", override.get("alias"))
        entity.add("notes", override.get("notes"))
    else:
        first_name = override.get("first_name")
        last_name = override.get("last_name")
        entity.id = context.make_id(first_name, last_name)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        h.apply_date(entity, "birthDate", override.get("dob"))
        entity.add("birthPlace", override.get("pob"))
        entity.add("idNumber", override.get("id_num"))
        entity.add("position", override.get("position"))
    # Reflects both a sanctions list and terrorist designations
    entity.add("topics", ["sanction", "crime.terror"])

    sanction = h.make_sanction(context, entity, program_name=PROGRAM)
    h.apply_date(sanction, "startDate", START_DATE)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
    doc = fetch_html(
        context,
        context.data_url,
        ".//div[contains(@class, 'wrapper')]",
        cache_days=1,
    )
    items = doc.xpath(".//div[@class='left level-5    type-bod-dd']")
    # We expect exactly 49 items, since it's a static page
    assert len(items) == 49
    for details_el in items:
        details = details_el.text_content()
        crawl_details(context, details)
