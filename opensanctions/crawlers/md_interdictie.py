from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from opensanctions.core import Context
from opensanctions import helpers as h

IDX_ORG_NAME = 1

def crawl(context: Context):
    path = context.fetch_resource("source.html", context.source.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    table = doc.find(".//table")
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = row.findall("./td")
            continue
        cells = row.findall("./td")
        
        name = ''.join(cells[IDX_ORG_NAME].itertext())
        print(name)
        
        entity = context.make("Company")
        entity.id = context.make_id(name)
        entity.add("name", name)

        context.emit(entity, target=True)