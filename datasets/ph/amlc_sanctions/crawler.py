import csv
from typing import Dict

from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    name = row.pop("name")
    name_raw = row.pop("original_string")
    alias = row.pop("alias")
    resolution_no = row.pop("resolution_no")
    program = row.pop("program")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, resolution_no)
    entity.add("name", name.split(";"), original_value=name_raw)
    entity.add("alias", alias.split(";") if alias else None, original_value=name_raw)
    entity.add("topics", "sanction")
    entity.add("country", "ph")
    entity.add("sourceUrl", row.pop("source_url"))
    entity.add("sourceUrl", row.pop("main_source_url"))
    context.emit(entity)

    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,  # program_key=program
    )
    sanction.add("program", resolution_no)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.url, cache_days=1)
    table = doc.xpath(".//div[@class='item-page']/table")
    assert len(table) == 1, "Expected exactly one table in the document"
    h.assert_dom_hash(table[0], "8ba11e0b3f463d5c4d5b2de281aaf9a23ac78104")
    # AMLC Resolution TF -108
    # AMLC Resolution TF -104
    # AMLC Resolution TF -102
    # AMLC Resolution TF -90
    # AMLC Resolution TF -88
    # AMLC Resolution TF -87
    # AMLC Resolution TF -86
    # AMLC Resolution TF -76
    # AMLC Resolution TF -69
    # AMLC Resolution TF -68
    # AMLC Resolution TF -67
    # AMLC Resolution TF -64
    # AMLC Resolution TF -63
    # AMLC Resolution TF -56
    # AMLC Resolution TF -55
    # AMLC Resolution TF -50
    # AMLC Resolution TF -42
    # AMLC Resolution TF -41
    # AMLC Resolution TF -40
    # AMLC Resolution TF -39
    # AMLC Resolution TF -35
    # AMLC Resolution TF -34
    # AMLC Resolution TF -33
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
