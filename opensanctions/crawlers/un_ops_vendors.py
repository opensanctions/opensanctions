from lxml import html
from pantomime.types import HTML

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_date(date):
    if date == "permanent":
        return None
    date = date.replace("Sept", "Sep")
    date = date.replace("ago", "Aug")
    return h.parse_date(date, ["%d-%b-%y", "%d-%b-%Y"])


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.dataset.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    for table in doc.findall("//table"):
        headers = table.findall("./thead/tr/td")
        headers = [h.text_content() for h in headers]
        assert "Vendor name" in headers, headers
        assert "From" in headers, headers
        for row in table.findall("./tbody/tr"):
            cells = [h.text_content() for h in row.findall("./td")]
            if len(cells[0]) == 0:
                continue
            entity = context.make("LegalEntity")
            entity.id = context.make_id(*cells)
            entity.add("name", cells[0])
            entity.add("country", cells[1])
            entity.add("topics", "crime.fraud")

            cc = entity.first("country")
            address = h.make_address(context, full=cells[2], country_code=cc)
            h.apply_address(context, entity, address)

            sanction = h.make_sanction(context, entity)
            sanction.add("reason", cells[3])
            sanction.add("program", cells[4])
            sanction.add("startDate", parse_date(cells[5]))
            sanction.add("endDate", parse_date(cells[6]))

            context.emit(sanction)

            context.emit(entity, target=True)
