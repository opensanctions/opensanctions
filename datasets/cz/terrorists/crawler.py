from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_details(context, details):
    result = context.lookup("details", details)
    if not result or not result.details:
        context.log.warning("Details are not parsed", details=details)

    override = result.details[0]
    name_org = override.get("name")
    first_name = override.get("first_name")
    last_name = override.get("last_name")

    entity = context.make(override.get("schema"))
    entity.id = context.make_id(name_org, first_name, last_name)
    if entity.schema.is_a("Organization"):
        entity.add("name", name_org)
        entity.add("alias", override.get("alias"))
        entity.add("notes", override.get("notes"))
    else:
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        h.apply_date(entity, "birthDate", override.get("dob"))
        entity.add("birthPlace", override.get("pob"))
        entity.add("idNumber", override.get("id_num"))
        entity.add("position", override.get("position"))

    context.emit(entity)


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
