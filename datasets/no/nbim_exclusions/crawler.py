from typing import Any, Dict, List
from normality import slugify, collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.util import ElementOrTree


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
            link = el.find("./a")
            if link is not None:
                value = collapse_spaces(link.text_content())
                cells.append((value, link.get("href")))
            else:
                value = collapse_spaces(el.xpath("string()"))
                cells.append(value)

        assert len(headers) == len(cells)
        rows.append({hdr: c for hdr, c in zip(headers, cells)})
    return rows


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)

    for data in parse_table(doc.find(".//table")):
        company = data.pop("company")
        if company is None:
            continue
        entity = context.make("Company")
        if len(company) == 2:
            name, url = company
        else:
            context.log.info("No link found for company", company=company)
            name, url = company, None
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("notes", data.pop(1) or None)
        decision = data.pop("decision")
        topic = context.lookup_value("decision_topic", decision)
        if topic is None:
            context.log.warning(f'Unexpected decision "{decision}"', decision=decision)
        entity.add("topics", topic)

        sanction = h.make_sanction(context, entity)
        sanction.add("description", decision)
        sanction.add("sourceUrl", url)
        sanction.add("program", data.pop("category"))
        sanction.add("reason", data.pop("criterion"))
        h.apply_date(sanction, "listingDate", data.pop("publishing-date"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(data)
