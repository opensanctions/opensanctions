from itertools import count
from time import sleep

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api

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


def crawl_entity(context: Context, *, url: str, name: str, category: str) -> None:
    context.log.info("Crawling entity", url=url)
    validator = ".//h1[contains(@class, 'page-title__heading')]"
    doc = zyte_api.fetch_html(context, url, validator, cache_days=7)
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

    body_els = h.xpath_elements(doc, ".//div[contains(@class, 'field--name-body')]")
    for body_el in body_els:
        body = h.element_text(body_el)
        entity.add("notes", body)

    contacts_containers = h.xpath_elements(
        doc, ".//div[contains(@class, 'field--name-field-public-alerts-contact')]"
    )
    if len(contacts_containers) > 1:
        context.log.warning(
            "Multiple contacts containers found for entity, don't know how to parse that",
            entity=entity,
        )

    if len(contacts_containers) == 1:
        # Splitting relies on the newlines, so we don't want to squash them.
        contacts = h.element_text(contacts_containers[0], squash=False)
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
                    res = context.lookup("contacts", row, warn_unmatched=True)
                    if res:
                        if res.prop:
                            entity.add(res.prop, row)
                        else:
                            continue
                    entity.add("notes", row)
                address.append(row)
        if address:
            entity.add("address", ", ".join(address))

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    table_xpath = ".//table[contains(@class, 'usa-table')]"

    for page in count(0):
        doc = zyte_api.fetch_html(
            context,
            context.data_url + f"?page={page}",
            table_xpath,
            cache_days=1,
            absolute_links=True,
        )

        table = h.xpath_elements(doc, table_xpath, expect_exactly=1)[0]
        # The first <tr> is the header, the second is the first row of data (or "No results.").
        if h.element_text(h.xpath_elements(table, ".//tr")[1]) == "No results.":
            break

        if page >= 100:
            raise RuntimeError(
                "Reached page 100 without exiting - are we in an infinite loop because the termination condition no longer triggers?"
            )

        for row in h.parse_html_table(table):
            sleep(SLEEP)
            name_a_el = h.xpath_element(row.pop("name"), ".//a")
            name, url = h.element_text(name_a_el), name_a_el.get("href")
            assert url is not None, "No URL found for name: %s" % name
            category = h.element_text(row.pop("category"))

            crawl_entity(context, url=url, name=name, category=category)
            context.audit_data(row)
