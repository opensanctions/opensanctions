from lxml import html
from normality import collapse_spaces
from pantomime.types import HTML

from opensanctions.core import Context
from opensanctions import helpers as h

CLEAN = [
    ", all its formations and front organizations.",
    ", all its formations and front organizations",
    ", All its formations and front organizations",
    ", All its formations and Front Organisations",
    "all its formations and front organizations.",
    ", and all its Manifestations",
]


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.source.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "rb") as fh:
        doc = html.fromstring(fh.read())
    for row in doc.findall('.//div[@class="field-content"]//table//tr'):
        cells = [c.text for c in row.findall("./td")]
        if len(cells) != 2:
            continue
        serial, name = cells
        if "Organisations listed in the Schedule" in name:
            continue
        entity = context.make("Organization")
        entity.id = context.make_id(serial, name)
        entity.add("topics", "sanction")
        for alias in name.split("/"):
            for clean in CLEAN:
                alias = alias.replace(clean, " ")
            entity.add("name", collapse_spaces(alias))

        context.emit(entity, target=True)
