import re

from dataclasses import dataclass, field
from itertools import chain
from banal import ensure_list
from lxml.etree import _Element
from typing import Dict, List, Optional

from zavod import Context, helpers as h


PROGRAM_KEY = "CA-SEMA"


@dataclass
class PersonBio:
    name: str
    name_ru: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    dob: Optional[str] = None


def parse_name_dob(name: str) -> PersonBio:
    name_ru = None
    dob = None
    aliases = []

    born_patterns = ["(born ", "(bornon "]
    for born in born_patterns:
        if born in name.lower():
            name, dob = name.split(born, 1)
            dob = dob.strip(")")

    if "(russian:" in name.lower():
        name, name_ru = name.split("(Russian:", 1)
        name_ru = name_ru.strip().rstrip(")")

    if "(also known as" in name.lower():
        name, aka = name.split("(also known as", 1)
        aliases = [a.strip().rstrip(")") for a in aka.split(",")]

    if "(également connue sous le nom" in name.lower():
        name, aka = name.split("(également connue sous le nom de ", 1)
        aliases = [a.strip().rstrip(")") for a in aka.split(",")]

    # some names contain digits at the beginning of the string
    name = re.sub(r"^\d+\s*", "", name)

    return PersonBio(
        name=name.strip(),
        dob=dob,
        name_ru=name_ru,
        aliases=aliases,
    )


def crawl_entity_notice(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    country = str_row.pop("country")
    listing_date = str_row.pop("date")
    schema_type = str_row.pop("types")
    res = context.lookup("schema_type", schema_type, warn_unmatched=True)
    schema = res.value if res else None
    if not schema:
        return

    specific_prohibition = row.pop("specific_prohibition")
    reason = h.xpath_strings(specific_prohibition, ".//p[3]//text()")
    people = h.xpath_strings(
        specific_prohibition, ".//div[contains(., 'List of individuals')]//li/text()"
    )
    entities = h.xpath_strings(
        specific_prohibition, ".//div[contains(., 'List of entities')]//li/text()"
    )
    for entity_name in chain(people, entities):
        entity = context.make(schema)
        entity.id = context.make_id(entity_name)
        parsed_bio = parse_name_dob(name=entity_name)

        entity.add("name", parsed_bio.name, lang="eng")
        entity.add("name", parsed_bio.name_ru, lang="rus")
        for alias in parsed_bio.aliases:
            # Aliases containing "or", "and", or "et" are ambiguous: they may be multiple
            # distinct names ("Helena Shudra or Victoria Pesti") or a single name with a
            # conjunction ("Radiological Chemical and Biological Defence troops"). Each case
            # is resolved via the datapatch lookup; unmatched cases are flagged.
            if re.search(r"\b(or|and|et)\b", alias, flags=re.I):
                res = context.lookup("name_alias", alias, warn_unmatched=True)
                if res:
                    for value in ensure_list(res.values):
                        entity.add("alias", value, lang="eng")
                    continue
            entity.add("alias", alias, lang="eng")

            # send the rest of the irregular aliases into review
            original = h.Names(name=alias)
            is_irregular, suggested = h.check_names_regularity(entity, original)

            h.review_names(
                context,
                entity,
                original=original,
                suggested=suggested,
                is_irregular=is_irregular,
            )

        if parsed_bio.dob:
            h.apply_date(entity, "birthDate", parsed_bio.dob)

        entity.add("topics", "sanction")
        sanction = h.make_sanction(
            context,
            entity,
            key=country,
            program_key=PROGRAM_KEY,
        )
        sanction.add("program", country)
        sanction.add("reason", reason)
        h.apply_date(sanction, "listingDate", listing_date)

        context.emit(entity)
        context.emit(sanction)


def crawl_vessel(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    imo_number = str_row.pop("ship_imo_number")
    name = str_row.pop("ship_name")
    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_number)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_number)
    vessel.add("type", str_row.pop("ship_type"))
    h.apply_date(vessel, "buildDate", str_row.pop("ship_build_date"))
    vessel.add("topics", "sanction")
    sanction = h.make_sanction(
        context,
        vessel,
        program_key=PROGRAM_KEY,
    )
    context.emit(vessel)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    entities_table = h.xpath_element(doc, '//table[contains(@id, "dataset-filter1")]')
    for row in h.parse_html_table(entities_table):
        crawl_entity_notice(context, row)

    ship_table = h.xpath_element(doc, '//table[contains(@class, "table wb-tables")]')
    for row in h.parse_html_table(ship_table):
        crawl_vessel(context, row)
