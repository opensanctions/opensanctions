import csv
from typing import Optional
from urllib.parse import urljoin
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h

SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTnxl3-xyO-9BBqM-rwJB849Kwm3-8ucrzVYZPl2-xhxky8DF4d485mrsyYyR266ePtK2-Qtpcz13jz/pub?gid=0&single=true&output=csv"


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    url: Optional[str] = None
    for link in doc.findall(".//div[@class='text-editor']//a"):
        url = urljoin(context.data_url, link.get("href"))
        url = url.replace("http://", "https://")
        if url and url.endswith(".pdf"):
            context.log.info(f"Found PDF: {url}")
            # There was a PDF; we've let them know it's gone.
            # The error says ERROR 404 but status is 200..
            content = context.fetch_text(url)
            if "Error 404" in content:
                # We'll just skip it since we know it's still a PDF link
                # but it's broken, i.e. no updates
                continue
            h.assert_url_hash(
                context,
                url,
                "75f76b1634bb3cfaffea8231c50cacab46371aff",
            )
    assert url, "No PDF found"

    path = context.fetch_resource("source.csv", SHEET_URL)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            entity = context.make("Person")
            name = row.pop("name_srp")
            entity.id = context.make_id(name)
            entity.add("name", name, lang="srp")
            entity.add("name", row.pop("name"), lang="eng")
            entity.add("alias", row.pop("name_alt"), lang="eng")
            entity.add("citizenship", row.pop("citizenship"), lang="eng")
            entity.add("birthPlace", row.pop("place_of_birth_srp"), lang="srp")
            entity.add("birthPlace", row.pop("place_of_birth"), lang="eng")
            h.apply_date(entity, "birthDate", row.pop("date_of_birth"))
            entity.add("birthPlace", row.pop("address_srp"), lang="srp")
            entity.add("birthPlace", row.pop("address"), lang="eng")
            entity.add("topics", "sanction")

            sanction = h.make_sanction(context, entity)
            sanction.add("sourceUrl", url)
            context.emit(entity)
            context.emit(sanction)
