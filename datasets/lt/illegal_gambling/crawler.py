import re
from typing import Generator, Dict, Tuple

from normality import collapse_spaces
from zavod import Context


def parse_table(
    table,
    skiprows=2,
    headers=["Number", "Name", "Domain", "Contacts", "Court information"],
) -> Generator[Dict[str, str | Tuple[str]], None, None]:
    '''
    The first two rows of the table represent the headers, but they are not using the th tag and
    so we are manually defining the column names.

    Args:
        table: The table element to parse
        skiprows: The number of rows to skip before parsing the table
        headers: The headers to use for the table columns
    Returns:
        A generator that yields a dictionary of the table columns and values. The keys are the
        column names and the values are the column values. If the column contains a link, the
        value is a tuple containing the link text and the link URL.
    Raises:
        AssertionError: If the number of headers and columns do not match. This indicates that the
        table is malformed.
    '''
    for row in table.findall(".//tr")[skiprows:]:
        cells = []
        for el in row.findall("./td"):
            a = el.find(".//a")
            if a is None:
                cells.append(collapse_spaces(el.text_content()))
            else:
                cells.append((collapse_spaces(a.text_content()), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


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

    for item in parse_table(
        response.find('.//*[@class="has-fixed-layout"]'), skiprows=2
    ):
        crawl_item(item, context)
