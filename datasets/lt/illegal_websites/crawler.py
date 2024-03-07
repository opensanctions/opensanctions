import csv
from typing import Dict
from pantomime.types import CSV
from zavod import Context
import shutil


def crawl_row(context: Context, row: Dict[str, str]):
    domain = row.pop("\ufeffDomenas")
    company_name = row.pop("Bendrovės pavadinimas")
    brand_name = row.pop("Prekės ženklas")

    entity = context.make("Organization")
    entity.id = context.make_slug(domain, prefix="lt-illegal-website")

    entity.add("name", company_name)
    entity.add("website", domain)
    entity.add("alias", brand_name)
    entity.add("topics", "crime.fin")

    context.emit(entity, target=True)
    context.audit_data(row)


def crawl(context: Context):
    # data is being read from a copy in the crawler directory as the source is cloudfare protected
    assert context.dataset.base_path is not None
    data_path = context.dataset.base_path / "data"
    path = context.get_resource_path("source.csv")
    shutil.copyfile(data_path, path)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh, delimiter=";"):
            crawl_row(context, row)
