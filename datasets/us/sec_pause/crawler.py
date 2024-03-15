from typing import Dict, Generator, Optional, Tuple
from lxml.etree import _Element
from normality import slugify, collapse_spaces

from zavod import Context
from zavod import helpers as h

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
    doc = context.fetch_html(url, cache_days=1)
    res = context.lookup("schema", category)
    if res is None or not isinstance(res.schema, str):
        context.log.warning("No schema found for category", category=category)
        return
    entity = context.make(res.schema)
    entity.id = context.make_id(name)
    entity.add("name", name)

    sanction = h.make_sanction(context, entity, key=category)
    sanction.add("program", category)

    body_el = doc.find(".//div[@class='article-body']")
    if body_el is not None:
        body = body_el.text_content().strip()
        entity.add("notes", body)

    container = doc.xpath(
        ".//div[contains(@class, 'stylized-box-1 public-alerts--unregistered-soliciting-entities-pause')]"
    )[0]
    container.remove(container.find(".//h2"))
    container.remove(container.find(".//h1"))
    contacts_container = container.findall("./")
    if len(contacts_container) != 1:
        context.log.warning(
            "Couldn't find single child contacts container",
            count=len(contacts_container),
        )
    else:
        contacts_container = contacts_container[0]
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

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    doc.make_links_absolute(context.data_url)

    table = doc.find(".//table")
    for row in parse_table(table):
        print(row)
        name, url = row.pop("name")
        if url is None:
            context.log.warning("No URL", name=name)
            continue
        name = name.replace("Name: ", "")
        category, _ = row.pop("category")
        category = category.replace("Category:", "").strip()
        crawl_entity(context, url, name, category)
        context.audit_data(row)
