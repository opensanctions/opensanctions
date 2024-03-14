import re
from typing import Generator, Dict, Tuple

from normality import collapse_spaces
from zavod import Context

EXPECTED_HEADERS = [
    [
        "Nr.",
        "Nelegalios lošimų veiklos vykdytojo duomenys",
        "Teismo, kuris išdavė leidimą duoti privalomą nurodymą tinklo paslaugų teikėjui ir/ar finansų institucijai, pavadinimas, nutarties priėmimo data, numeris",
    ],
    [
        "Pavadinimas",
        "Interneto domeno vardas, identifikuojantis interneto svetainę",
        "Kontaktiniai duomenys (el. paštas, tel. Nr.)",
    ],
]
HEADERS = ["Number", "Name", "Domain", "Contacts", "Court information"]


def parse_table(
    table,
) -> Generator[Dict[str, str | Tuple[str]], None, None]:
    """
    The first two rows of the table represent the headers, but we're not going to
    try and parse colspan and rowspan.

    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values.
    Raises:
        AssertionError: If the headers don't match what we expect.
    """

    for row_ix, row in enumerate(table.findall(".//tr")):
        cells = [collapse_spaces(cell.text_content()) for cell in row.findall("./td")]
        if row_ix < len(EXPECTED_HEADERS):
            assert cells == EXPECTED_HEADERS[row_ix], cells
            continue
        assert len(cells) == len(HEADERS), cells
        yield {hdr: c for hdr, c in zip(HEADERS, cells, strict=True)}


def crawl_item(item, context: Context):
    name = item.pop("Name")
    domain = item.pop("Domain")
    contacts = item.pop("Contacts")
    ruling_information = item.pop("Court information")

    entity = context.make("Company")
    entity.id = context.make_slug(domain)

    entity.add("name", name)
    entity.add("website", domain)

    # We find all emails in the contacts field and add them to the entity
    if isinstance(contacts, tuple):
        for contact in contacts:
            emails = re.findall(r"[\w\.-]+@[\w\.-]+", contact)
            for email in emails:
                entity.add("email", email)
    else:
        emails = re.findall(r"[\w\.-]+@[\w\.-]+", contacts)
        for email in emails:
            entity.add("email", email)

    entity.add("topics", "crime")
    entity.add("notes", f"Pripažino nelegaliu lošimų organizatoriumi: {ruling_information}", lang="lit")

    context.emit(entity, target=True)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)
    tables = response.findall('.//table')
    for table in tables:
        first_row = table.find('.//tr')
        if not "Nelegalios lošimų veiklos vykdytojo duomenys" in first_row.text_content():
            continue
        for item in parse_table(table):
            crawl_item(item, context)