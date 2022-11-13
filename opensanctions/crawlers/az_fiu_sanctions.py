from lxml import html
from pantomime.types import HTML
from normality import slugify, collapse_spaces

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_date(date):
    return h.parse_date(date, ["%m/%d/%Y"])


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.source.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)
    table = doc.find('//div[@class="sanctioned-table"]/table')
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(el.text) for el in row.findall("./th")]
            continue
        cells = [collapse_spaces(el.text) for el in row.findall("./td")]
        data = {hdr: c for hdr, c in zip(headers, cells)}

        entity = context.make("Person")
        entity.id = context.make_id(data["id"], data["ad-soyad-ata-adi"])
        entity.add("name", data["ad-soyad-ata-adi"])
        entity.add("idNumber", data["id"])
        entity.add("birthDate", parse_date(data["dogum-tarixi"]))
        entity.add("country", "az")
        entity.add("topics", "sanction")

        addr = h.make_address(context, full=data["malumat"])
        h.apply_address(context, entity, addr)

        sanction = h.make_sanction(context, entity)
        context.emit(sanction)
        context.emit(entity, target=True)
