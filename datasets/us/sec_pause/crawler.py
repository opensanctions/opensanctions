from typing import Dict, Generator, Optional, Tuple
from lxml.etree import _Element
from normality import slugify, collapse_spaces
from time import sleep

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

# Ensure never more than 10 requests per second
# https://www.sec.gov/about/privacy-information#security
SLEEP = 0.1

CONTACTS = [
    ("Phone:", "Phone:", "phone"),
    ("Phone", "Phone", "phone"),
    ("Telefax:", "Telefax:", "phone"),
    ("Tel:", "Tel:", "phone"),
    ("Telephone:", "Telephone:", "phone"),
    ("Fax", "", "notes"),
    ("Email:", "Email:", "email"),
    ("E-mail:", "E-mail:", "email"),
    ("Website:", "Website:", "website"),
    ("Website", "Website", "website"),
    ("Web:", "Web:", "website"),
    ("www", "", "website"),
    ("http", "", "website"),
    ("Direct:", "Direct:", "phone"),
]


def parse_table(
    table: _Element,
) -> Generator[Dict[str, Tuple[str, Optional[str]]], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                headers.append(slugify(el.text_content()))
            continue
        cells = []
        for el in row.findall("./td"):
            a = el.find(".//a")
            if a is None:
                cells.append((collapse_spaces(el.text_content()), None))
            else:
                cells.append((collapse_spaces(a.text_content()), a.get("href")))

        assert len(headers) == len(cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_entity(context: Context, url: str, name: str, category: str) -> None:
    context.log.info("Crawling entity", url=url)
    validator = ".//h1[contains(@class, 'page-title__heading')]"
    doc = fetch_html(context, url, validator, cache_days=7)
    res = context.lookup("schema", category)
    if res is None or not isinstance(res.schema, str):
        context.log.warning("No schema found for category", category=category)
        return
    entity = context.make(res.schema)
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("topics", "crime.fraud")
    entity.add("sourceUrl", url)

    sanction = h.make_sanction(context, entity, key=category)
    sanction.add("program", category)

    body_els = doc.xpath(".//div[contains(@class, 'field--name-body')]")
    if body_els:
        body = body_els[0].text_content().strip()
        entity.add("notes", body)

    contacts_containers = doc.xpath(
        ".//div[contains(@class, 'field--name-field-public-alerts-contact')]"
    )
    if contacts_containers:
        contacts_container = contacts_containers[0]
        contacts = contacts_container.text_content()
        contacts = contacts.replace(" :", ":")
        address = []
        for row in contacts.split("\n"):
            value = None
            row = row.strip()
            if row == "":
                continue
            for prefix, replace, prop_name in CONTACTS:
                if row.startswith(prefix):
                    value = row.replace(replace, "").strip()
                    entity.add(prop_name, value)
                    break
            if value is None:
                if ":" in row:
                    res = context.lookup("contacts", row)
                    if res:
                        if res.prop:
                            entity.add(res.prop, row)
                        else:
                            continue
                    context.log.warn(
                        "possible non-address line slipping through", line=row
                    )
                    entity.add("notes", row)
                address.append(row)
        if address:
            entity.add("address", ", ".join(address))

    context.emit(entity)
    context.emit(sanction)


def index_unblock_validator(doc: _Element) -> bool:
    return len(doc.xpath(".//table[contains(@class, 'usa-table')]")) > 0


def crawl(context: Context) -> None:
    table_xpath = ".//table[contains(@class, 'usa-table')]"
    doc = fetch_html(context, context.data_url, table_xpath, cache_days=1)
    doc.make_links_absolute(context.data_url)

    table = doc.xpath(table_xpath)[0]
    for row in parse_table(table):
        sleep(SLEEP)
        name, url = row.pop("name")
        if url is None:
            context.log.warning("No URL", name=name)
            continue
        name = name.replace("Name: ", "")
        category, _ = row.pop("category")
        category = category.replace("Category:", "").strip()
        crawl_entity(context, url, name, category)
        context.audit_data(row)
