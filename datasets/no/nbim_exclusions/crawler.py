import re
from typing import List
from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context, Entity
from zavod import helpers as h


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    table = doc.find(".//table")
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(el.text_content()) for el in row.findall("./th")]
            continue

        cells = []
        for el in row.findall(".//span[@class='nbim-responsive-table--value']"):
            link_value = el.find("./a")
            if link_value is not None:
                cells.append(
                    (collapse_spaces(link_value.text_content()), link_value.get("href"))
                )
            else:
                cells.append(collapse_spaces(el.text_content()))
        data = {hdr: c for hdr, c in zip(headers, cells)}

        entity = context.make("Company")
        name, url = data.pop("company")
        entity.id = context.make_slug(name)
        entity.add("name", name)

        sanction = h.make_sanction(context, entity)
        sanction.add("sourceUrl", url)

        context.emit(entity, target=True)
        context.emit(sanction)
