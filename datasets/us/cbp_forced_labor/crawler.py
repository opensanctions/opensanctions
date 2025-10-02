import csv

from rigour.mime.types import CSV
from zavod.shed.zyte_api import fetch_html, fetch_resource

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: dict):
    country = row.pop("Country")
    sector = row.pop("Industry")
    status = row.pop("Status")
    wro = row.pop("WRO/Finding")
    remarks = row.pop("Remarks")
    source_url = row.pop("Press Release")
    start_date = row.pop("Effective Date")
    name = row.pop("Entity")
    name_result = context.lookup("name", name)
    if name_result is None:
        context.log.warning("No name found for company", name_result=name)
        return
    for match_entity in name_result.entities:
        if not match_entity.get("name"):
            context.log.warning("No name found for a company", entity=match_entity)
            continue
        schema = name_result.schema or "LegalEntity"
        entity = context.make(schema)
        entity.id = context.make_id(match_entity.get("name"), country)
        entity.add("name", match_entity.get("name"))
        for prop, value in match_entity.items():
            entity.add(prop, value)
        entity.add("country", country)
        entity.add("sourceUrl", source_url)
        entity.add("notes", remarks)
        entity.add("description", wro)
        if entity.schema.is_a("Vessel"):
            entity.add("keywords", sector)
        else:
            entity.add("sector", sector)

        if status in ["Active", "Partially Active"]:
            entity.add("topics", "sanction")
            sanction = h.make_sanction(context, entity)
            h.apply_date(sanction, "startDate", start_date)
            context.emit(sanction)

        context.emit(entity)
        context.audit_data(row, ignore=["Calendar Year", "Country Code"])


def crawl(context: Context):
    csv_xpath = "//a[(contains(., 'Withold') or contains(., 'Withhold')) and contains(., 'Dataset')]/@href"
    doc = fetch_html(context, context.data_url, csv_xpath, absolute_links=True)
    csv_url = doc.xpath(csv_xpath)
    assert len(csv_url) == 1, len(csv_url)
    csv_url = csv_url[0]

    _, _, _, path = fetch_resource(context, "source.csv", csv_url, CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
