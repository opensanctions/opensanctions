import re
from typing import Iterator

from zavod import Context
from zavod import helpers as h
from zavod.util import Element

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
NAME_SPLITS = [
    "“, „",
    "”, „",  # This really does differ from the one above
    "“; „",
    "“ „",
    "“), „",
    "“",
    "”",
    "(„",
    "„",
]


def parse_table(
    table: Element,
) -> Iterator[dict[str, str]]:
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
        cells = [h.element_text(cell) for cell in row.findall("./td")]
        if row_ix < len(EXPECTED_HEADERS):
            assert cells == EXPECTED_HEADERS[row_ix], cells
            continue
        assert len(cells) == len(HEADERS), cells
        yield {hdr: c for hdr, c in zip(HEADERS, cells, strict=True)}


def crawl_item(item: dict[str, str], context: Context) -> None:
    raw_names = item.pop("Name")
    domain = item.pop("Domain")
    contacts = item.pop("Contacts")
    ruling_information = item.pop("Court information")

    # Skip empty row at the end of the table
    if not any([raw_names, domain, contacts, ruling_information]):
        return

    entity = context.make("Company")
    entity.id = context.make_id(raw_names)

    for name in h.multi_split(raw_names, NAME_SPLITS):
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
    entity.add(
        "notes",
        f"Pripažino nelegaliu lošimų organizatoriumi: {ruling_information}",
        lang="lit",
    )

    context.emit(entity)


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url)
    tables = response.findall(".//table")
    for table in tables:
        first_row = table.find(".//tr")
        if "Nelegalios lošimų veiklos vykdytojo duomenys" not in h.element_text(
            first_row
        ):
            continue
        for item in parse_table(table):
            crawl_item(item, context)
