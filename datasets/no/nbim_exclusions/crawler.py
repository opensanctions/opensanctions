import re
from typing import Any, Dict, List
from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context, Entity
from zavod import helpers as h
from zavod.util import ElementOrTree


DATE_FORMATS = ["%d.%m.%Y"]


def parse_table(table: ElementOrTree) -> List[Dict[str, Any]]:
    rows = []
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for i, el in enumerate(row.findall("./th")):
                slug = slugify(el.text_content())
                if slug is None:
                    headers.append(i)
                else:
                    headers.append(slug)
            continue

        cells = []
        for el in row.findall(".//td"):
            value = el.find(".//span[@class='nbim-responsive-table--value']")
            link = value.find("./a") if value is not None else None
            if link is None:
                cells.append(
                    collapse_spaces(value.text_content()) if value is not None else None
                )
            else:
                cells.append((collapse_spaces(link.text_content()), link.get("href")))

        assert len(headers) == len(cells)
        rows.append({hdr: c for hdr, c in zip(headers, cells)})
    return rows



def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    for data in parse_table(doc.find(".//table")):
        entity = context.make("Company")
        name, url = data.pop("company")
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("notes", data.pop(1) or None)

        sanction = h.make_sanction(context, entity)

        decision = data.pop("decision")
        sanction.add("description", decision)
        match decision:
            case "Exclusion":
                entity.add("topics", "debarment")
            case "Observation":
                pass
            case _:
                context.log.warning("Unexpected decision", decision=decision)

        sanction.add("sourceUrl", url)
        sanction.add("program", data.pop("category"))
        sanction.add("reason", data.pop("criterion"))
        sanction.add(
            "listingDate", h.parse_date(data.pop("publishing-date"), DATE_FORMATS)
        )

        context.emit(entity, target=True)
        context.emit(sanction)

        context.audit_data(data)
