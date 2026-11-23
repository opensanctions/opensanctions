import csv

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h


def parse_countries(countries: str) -> list[str]:
    return [c.strip() for c in countries.split(", ") if c.strip()]


def crawl_row(context: Context, row: dict[str, str]) -> None:
    # The source mis-classifies some entities, so this only feeds the ID, not the schema.
    entity_type = row.pop("Entity")
    assert entity_type in ("Firm", "Individual"), entity_type
    title = row.pop("Title")
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(entity_type, title)
    entity.add("name", title)
    for country in parse_countries(row.pop("Country")):
        entity.add("country", country)
    for country in parse_countries(row.pop("Nationality")):
        entity.add("country", country)

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop("Prohibited Practice"))
    sanction.add("authority", row.pop("Source"))
    sanction.add("program", row.pop("Tipo de sancion del BID"))
    h.apply_date(sanction, "startDate", row.pop("From"))
    # "Ongoing" is mapped to no end date in the type.date lookup.
    h.apply_date(sanction, "endDate", row.pop("To"))

    # "Other Name" seems to be another name for the sanction source, not for
    # the entity: it spells out the code in "IDB Sanction Source". Pinning the
    # known pairs makes the crawl fail if entity aliases appear in it.
    source_code = row.pop("IDB Sanction Source")
    source_label = row.pop("Other Name")
    assert (source_code, source_label) in [
        ("", ""),
        ("SCOM", "Sanctions Committee"),
        ("SOFF", "Sanctions Officer"),
        ("NRAG", "NRA (Negotiated Resolution Agreement)"),
    ], (source_code, source_label)
    sanction.add("authority", source_label)

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row, ignore=["_id"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
