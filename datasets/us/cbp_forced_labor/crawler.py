from rigour.mime.types import CSV
import csv

from zavod import Context, helpers as h


def crawl_row(context: Context, row: dict):
    country = row.pop("Country")
    sector = row.pop("Industry")
    status = row.pop("Status")
    merchandise = row.pop("Merchandise")
    wro = row.pop("WRO/Finding")
    remarks = row.pop("Remarks")
    source_url = row.pop("Press Release")
    start_date = row.pop("Effective Date")
    modified_date = row.pop("Modified Date")
    name = row.pop("Entity")
    name_result = context.lookup("name", name)
    if name_result is None:
        context.log.warning("No name found for company", name_result=name)
        return
    for match_entity in name_result.entities:
        if not match_entity.get("name"):
            context.log.warning("No name found for a company", entity=match_entity)
            continue
        schema = "Vessel" if "Fishing Vessels" in country else "LegalEntity"
        entity = context.make(schema)
        entity.id = context.make_id(match_entity.get("name"))
        entity.add("name", match_entity.get("name"))
        for prop, value in match_entity.items():
            entity.add(prop, value)
        entity.add("country", country)
        entity.add("sourceUrl", source_url)
        entity.add("notes", remarks)
        entity.add("description", wro)
        if entity.schema.is_a("Vessel"):
            entity.add("keywords", sector)
            entity.add("keywords", merchandise)
        else:
            entity.add("sector", sector)
            entity.add("sector", merchandise)

        if status in ["Active", "Partially Active"]:
            entity.add("topics", "sanction")
            sanction = h.make_sanction(context, entity)
            h.apply_date(sanction, "startDate", start_date)
            h.apply_date(sanction, "modifiedAt", modified_date)
            context.emit(sanction)

        context.emit(entity)
        context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    doc.make_links_absolute(context.data_url)
    csv_url = doc.xpath(
        "//table//tr[th[contains(., 'Withhold Release Orders & Findings Dataset')]]//@href"
    )
    assert len(csv_url) == 1, "Expected exactly one CSV url"
    csv_url = csv_url[0]

    path = context.fetch_resource("source.csv", csv_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
