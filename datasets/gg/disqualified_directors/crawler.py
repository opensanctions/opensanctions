from typing import Generator, Dict
from lxml.etree import _Element
from normality import collapse_spaces

from zavod import Context, helpers as h


def parse_table(table: _Element) -> Generator[Dict[str, str], None, None]:
    """
    Parse the table and returns the information as a list of dict

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """
    headers = [th.text_content() for th in table.findall(".//*/th")]
    for row in table.findall(".//*/tr")[1:]:
        cells = []
        for el in row.findall(".//td"):
            cells.append(collapse_spaces(el.text_content()))
        assert len(cells) == len(headers)

        # The table has a last row with all empty values
        if all(c == "" for c in cells):
            continue

        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_item(item: Dict[str, str], context: Context):
    name = item.pop("Name of disqualified director")

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add("topics", "corp.disqual")

    sanction = h.make_sanction(context, person)
    sanction.add(
        "startDate",
        h.parse_date(item.pop("Date of disqualification"), formats=["%d.%m.%Y"]),
    )
    sanction.add("authority", item.pop("Applicant for disqualification"))
    sanction.add("duration", item.pop("Period of disqualification"))
    sanction.add(
        "endDate",
        h.parse_date(item.pop("End of disqualification period"), formats=["%d.%m.%Y"]),
    )

    context.emit(person, target=True)
    context.emit(sanction)

    context.audit_data(item)


def crawl(context: Context) -> None:

    response = context.fetch_html(context.data_url)

    for item in parse_table(response.find(".//table")):
        crawl_item(item, context)
