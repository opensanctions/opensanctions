from normality import slugify

from zavod import Context
from zavod import helpers as h
from zavod.util import Element, ElementOrTree

# Cell value: either plain text or (link_text, href) for linked cells
CellValue = str | None | tuple[str | None, str | None]
# Row dict: string keys for named headers, int keys for unnamed columns
TableRow = dict[str | int, CellValue]


def parse_table(table: ElementOrTree) -> list[TableRow]:
    """Parse an HTML table into a list of row dicts.

    Header cells without a slug get an integer index as key.
    Cells containing a link yield a (text, href) tuple; plain cells yield str | None.
    """
    rows: list[TableRow] = []
    headers: list[str | int] | None = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for i, el in enumerate(row.findall("./th")):
                slug = slugify(h.element_text(el))
                if slug is None:
                    headers.append(i)
                else:
                    headers.append(slug)
            continue

        cells: list[CellValue] = []
        for el in row.findall(".//td"):
            link: Element | None = el.find("./a")
            if link is not None:
                value = h.element_text(link) or None
                cells.append((value, link.get("href")))
            else:
                cells.append(h.element_text(el) or None)

        assert len(headers) == len(cells)
        rows.append({hdr: c for hdr, c in zip(headers, cells)})
    return rows


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = doc.find(".//table")
    assert table is not None, "No table found on page"
    for data in parse_table(table):
        company = data.pop("company")
        if company is None:
            continue
        entity = context.make("Company")
        if isinstance(company, tuple) and len(company) == 2:
            name, url = company
        else:
            context.log.info("No link found for company", company=company)
            name, url = company, None
        entity.id = context.make_slug(name)
        entity.add("name", name)
        entity.add("notes", data.pop(1) or None)
        decision = data.pop("decision")
        assert isinstance(decision, str) or decision is None, (
            f"Expected plain text for decision, got {decision!r}"
        )
        topic = context.lookup_value("decision_topic", decision)
        if topic is None:
            context.log.warning(f'Unexpected decision "{decision}"', decision=decision)
        entity.add("topics", topic)

        sanction = h.make_sanction(context, entity)
        sanction.add("description", decision)
        sanction.add("sourceUrl", url)
        sanction.add("program", data.pop("category"))
        sanction.add("reason", data.pop("criterion"))
        listing_date = data.pop("publishing-date")
        assert isinstance(listing_date, str) or listing_date is None, (
            f"Expected plain text for publishing-date, got {listing_date!r}"
        )
        h.apply_date(sanction, "listingDate", listing_date)

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(data)
