import csv
from typing import Dict
from pantomime.types import CSV

from zavod import Context
from zavod.shed.zyte_api import fetch_resource


def crawl_row(context: Context, row: Dict[str, str]):
    domain = row.pop("\ufeffDomenas")
    company_name = row.pop("Bendrovės pavadinimas")
    brand_name = row.pop("Prekės ženklas")

    entity = context.make("Organization")
    entity.id = context.make_slug(company_name or domain)

    entity.add("name", company_name or brand_name or domain)
    entity.add("website", domain)
    entity.add("alias", brand_name if not company_name else None)
    entity.add("topics", "crime.fin")

    context.emit(entity, target=True)
    context.audit_data(row)


def crawl(context: Context):
    cached, path, type, charset = fetch_resource(
        context, "source.csv", context.data_url
    )
    assert cached or type == CSV, type
    assert cached or charset == "utf-8", charset
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh, delimiter=";"):
            crawl_row(context, row)
