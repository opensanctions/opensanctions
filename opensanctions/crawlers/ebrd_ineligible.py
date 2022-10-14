from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from opensanctions.core import Context
from opensanctions import helpers as h

FORMATS = ["%d %b %Y", "%d-%b-%Y"]


def parse_date(text):
    text = text.replace("Sept", "Sep")
    text = text.replace("July", "Jul")
    return h.parse_date(text, FORMATS)


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.dataset.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    table = doc.find(".//article//table")
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(c.text_content(), "_") for c in row.findall("./td")]
            headers = headers[:-2] + ["from", "to"] + headers[-1:]
            continue
        cells = [collapse_spaces(c.text_content()) for c in row.findall("./td")]
        cells = dict(zip(headers, cells))
        if "prohibited_practice" not in cells:
            continue

        name = cells.pop("firm_name")
        nationality = cells.pop("nationality")
        entity = context.make("Company")
        entity.id = context.make_id(name, nationality)
        entity.add("name", name)
        entity.add("topics", "debarment")
        entity.add("country", nationality)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells.pop("prohibited_practice"))
        sanction.add("startDate", parse_date(cells.pop("from")))
        sanction.add("endDate", parse_date(cells.pop("to")))

        full = cells.pop("address")
        address = h.make_address(context, full=full, country=nationality)
        h.apply_address(context, entity, address)

        context.emit(entity, target=True)
        context.emit(sanction)
