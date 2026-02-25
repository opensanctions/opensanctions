from lxml.etree import _Element
from datetime import datetime
import re

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
        assert name is not None
        name = name.strip()

        entity = context.make(schema)
        entity.id = context.make_id(name)

        # --- clean the name + extract DOB if present
        if "(born " in name.lower():
            dirty_dob = name.split("(born")[1].strip()
            name = name.split("(born")[0].strip()
            entity.add("name", name)

            match_full_date = re.search(r"[A-Z][a-z]+ \d{1,2}, \d{4}", dirty_dob)
            if match_full_date:
                dob = datetime.strptime(match_full_date.group(), "%B %d, %Y")
                entity.add("birthDate", dob)
            match_year = re.search(r"\d{4}", dirty_dob)
            if match_year:
                dob = datetime.strptime(match_year.group(), "%Y")
                entity.add("birthDate", dob)

        # --- clean up aliases with Russian names
        if "(russian:" in name.lower():
            alias = name.split("(Russian:")[1].strip().replace(")", "")
            name = name.split("(Russian:")[0].strip()
            entity.add("name", name)
            entity.add("alias", alias)

        # send the rest of the irregular names into review
        original = h.Names(name=name)
        is_irregular, suggested = h.check_names_regularity(entity, original)
        if is_irregular:
            print(name)

        h.review_names(
            context,
            entity,
            original=original,
            suggested=suggested,
            is_irregular=is_irregular,
        )
        # use h.apply_reviewed_names() once the reviews are done?

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

        context.emit(entity)
        context.emit(sanction)


def parse_ships(context: Context, row: _Element) -> None:
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
        program_key="CA-SEMA",
    )
    # sanction.add("program", country)
    date = datetime.today().strftime("%B %d, %Y")

    h.apply_date(
        sanction, "listingDate", date
    )  # double check which property i should put the today() date into for ships (temp date for ships)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)

    # --- crawl people and entities ---
    rows_entities_persons = h.xpath_elements(
        doc, '//table[contains(@id, "dataset-filter1")]/tbody/tr'
    )
    for row in rows_entities_persons:
        parse_entities_persons(context, row)

    # --- crawl ships ---
    # country_ships = h.xpath_strings(doc, '//h2[contains(., "Regulations against ships")]/following::p[contains(., "Country")]/text()')
    # date_ships = h.xpath_strings(doc, '//h2[contains(., "Regulations against ships")]/following::p[contains(., "Entered into force")]/text()')

    rows_ships = h.xpath_elements(
        doc, '//table[contains(@class, "table wb-tables")]/tbody/tr'
    )
    for row in rows_ships:
        parse_ships(context, row)
