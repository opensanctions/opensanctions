from lxml.etree import _Element
from zavod import Context, helpers as h


def parse_entities_persons(context: Context, row: _Element) -> None:
    country = h.xpath_string(row, ".//p[contains(., 'Country')]/text()").strip()
    date = h.xpath_string(row, ".//p[contains(., 'Entered into force')]/text()").strip()

    parts = h.xpath_strings(row, ".//p[3]//text()")  # some entries embed <em>s
    notes = " ".join(parts).strip()

    people_list = list(
        h.xpath_elements(row, ".//div[contains(., 'List of individuals')]//li")
    )
    entities_list = list(
        h.xpath_elements(row, ".//div[contains(., 'List of entities')]//li")
    )

    lis, schema = (
        (people_list, "Person") if people_list else (entities_list, "LegalEntity")
    )

    for li in lis:
        name = li.text
        entity = context.make(schema)
        entity.id = context.make_id(name)  # TODO: add country, date?
        entity.add("name", name)
        # entity.add("country", country)  # not sure if correct to add here bc country is for sanctions
        entity.add("notes", notes)
        entity.add("topics", "sanction")

        sanction = h.make_sanction(
            context,
            entity,
            program_name=country,
            source_program_key=country,
            program_key="CA-SEMA",
        )
        sanction.add("program", country)
        h.apply_date(sanction, "listingDate", date)


def parse_ships(context: Context, row: _Element, country: str, date: str) -> None:
    imo_number = h.xpath_string(row, ".//td[2]//text()").strip()
    name = h.xpath_string(row, ".//td[1]//text()").strip()

    entity = context.make("Vessel")
    entity.id = context.make_id(name, imo_number)
    entity.add("name", name)
    entity.add("imoNumber", imo_number)
    entity.add("type", h.xpath_string(row, ".//td[3]//text()").strip())
    h.apply_date(entity, "buildDate", h.xpath_string(row, ".//td[4]//text()").strip())

    entity.add("topics", "sanction")
    sanction = h.make_sanction(
        context,
        entity,
        program_name=country,
        source_program_key=country,
        program_key="CA-SEMA",
    )
    sanction.add("program", country)
    h.apply_date(sanction, "listingDate", date)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)

    rows_entities_persons = h.xpath_elements(
        doc, '//table[contains(@id, "dataset-filter1")]/tbody/tr'
    )
    for row in rows_entities_persons:
        parse_entities_persons(context, row)

    country_ships = h.xpath_string(
        doc,
        '//h2[contains(., "Regulations against ships")]/following::p[contains(., "Country")]/text()',
    ).strip()
    date_ships = h.xpath_string(
        doc,
        '//h2[contains(., "Regulations against ships")]/following::p[contains(., "Entered into force")]/text()',
    ).strip()

    rows_ships = h.xpath_elements(
        doc, '//table[contains(@class, "table wb-tables")]/tbody/tr'
    )
    for row in rows_ships:
        parse_ships(context, row, country=country_ships, date=date_ships)


# TODO:
# - split by AKAs (rigour) + alias lookups
# - Rejected property value [listingDate]: February 21 2025
