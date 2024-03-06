import csv
from typing import Dict
from pantomime.types import CSV
from zavod import Context
import shutil


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Cookie": "_fbp=fb.1.1709277122561.2019162693; cookie-save-select=1384497; cf_clearance=tkiu2Je8N4dgJR8aK_gj1jVFU3VAchi1vDPRiWIXEhE-1709732810-1.0.1.1-TvBQR1yjipT9yy9wMVVBjhvW7UMEA0tkdrk2y.kixw8zMuk_OGD7wN9ds_rNtTSUJacd9rqTR5E.AzzWOyOKGA; PHPSESSID=e600ea36bc79177aefd31c04ea7526e9; __cflb=02DiuJFtw57keZiWgHa69KQ1Wu4g8NNCEk4qkPtdrYmji",
}


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

    # path = context.fetch_resource("source.csv", context.data_url, headers=HEADERS)

    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh, delimiter=";"):
            crawl_row(context, row)
