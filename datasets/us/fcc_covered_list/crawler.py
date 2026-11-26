import csv
import shutil
from rigour.mime.types import CSV
from pathlib import Path

from zavod import Context, helpers as h
from zavod.extract import zyte_api

PROGRAM_KEY = "US-FCC"


def crawl_item(context: Context, row: dict[str, str]) -> None:
    name = row.pop("entity_name")
    description = row.pop("covered_equipment_or_services")
    inclusion_date = row.pop("date_of_inclusion_on_covered_list")
    updated_date = row.pop("updated")
    source_url = row.pop("source_url")
    if source_url == "":
        raise ValueError(f"Row for {name!r} has no source_url")

    entity = context.make("Company")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("topics", "debarment")

    for subsidiary_name in row.pop("subsidiary_names").split(";"):
        subsidiary_name = subsidiary_name.strip()
        if subsidiary_name == "":
            continue

        subsidiary = context.make("Company")
        subsidiary.id = context.make_slug(subsidiary_name)
        subsidiary.add("name", subsidiary_name)
        subsidiary.add("topics", "debarment")

        ownership = context.make("Ownership")
        ownership.id = context.make_id(name, subsidiary_name)
        ownership.add("asset", subsidiary)
        ownership.add("owner", entity)

        subsidiary_sanction = h.make_sanction(context, subsidiary)
        subsidiary_sanction.add("description", description)
        subsidiary_sanction.add("sourceUrl", source_url)
        h.apply_date(subsidiary_sanction, "listingDate", inclusion_date)
        h.apply_date(subsidiary_sanction, "modifiedAt", updated_date)
        h.finalize_sanction(context, subsidiary_sanction, effective_on_listing=True)

        context.emit(subsidiary)
        context.emit(ownership)
        context.emit(subsidiary_sanction)

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    sanction.add("description", description)
    sanction.add("description", row.pop("footnote_1"))
    sanction.add("sourceUrl", source_url)
    h.apply_date(sanction, "listingDate", inclusion_date)
    h.apply_date(sanction, "modifiedAt", updated_date)
    h.finalize_sanction(context, sanction, effective_on_listing=True)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    assert context.dataset.url is not None
    table_xpath = './/table[.//th[contains(., "Date of Inclusion on Covered List")]]'
    # The rendered page gains hidden "Visit - " spans from a third-party
    # accessibility script in some renders but not others, which flips the hash.
    doc = zyte_api.fetch_html(
        context, context.dataset.url, table_xpath, html_source="httpResponseBody"
    )
    table = h.xpath_element(doc, table_xpath)
    with open(Path(__file__).parent / "table.csv", "w") as fh:
        rows = [h.cells_to_str(r) for r in h.parse_html_table(table)]
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    # The entities in covered_list.csv are transcribed by hand from this table. The
    # runbook for a changed hash is in the dataset YAML.
    h.assert_dom_hash(table, "6562ffeeba8bf24c94e2fcac3ee58ea9ac69c78e", text_only=True)

    source_file = Path(__file__).parent / "covered_list.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, title=context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            crawl_item(context, row)
