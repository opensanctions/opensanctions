from csv import DictReader
from typing import Dict, List, cast

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.review import assert_all_accepted


# count of identifiers by number of parts (split on opening paren)
# 1: 4334 - no parentheses
# 2: 609 - single set of parentheses
# 3: 1369
# 4: 387
# ...
# could benefit from LLM extraction e.g.
# "02800443 (id-National identification card) ((identity card))"
# "02810614 (id-National identification card) (ID no)"
# "03 01  118013 (other-Other identification number) (Passport number,  national ID  number, other numbers of  identity  documents: 03 01  118013)",
# "0363464 (passport-National passport)  (issued by Palestinian Authority)"
def apply_identifier(context: Context, entity: Entity, id_number_line: str):
    parts = id_number_line.split("(")
    prop = None

    if len(parts) == 1:
        prop = "idNumber"
    if len(parts) == 2:
        prop = context.lookup_value(
            "identifier_prop", parts[1].strip(") "), warn_unmatched=True
        )

    if prop and prop in entity.schema.properties:
        entity.add(prop, parts[0].strip(), original_value=id_number_line)
    else:
        entity.add("notes", id_number_line)


def crawl_row(context: Context, entity_id: str | None, row: Dict[str, List[str]]):
    schema = context.lookup_value(
        "subject_type",
        row.pop("type")[0],
        default="LegalEntity",
        warn_unmatched=True,
    )
    if schema is None:
        return  # The lookup warns
    entity = context.make(schema)
    entity.id = entity_id

    whole_names = row.pop("Wholename")
    assert whole_names, row
    original = h.Names(name=cast(list[str | None], whole_names))
    h.apply_reviewed_names(context, entity, original=original)

    for id_number_line in row.pop("Number"):
        apply_identifier(context, entity, id_number_line)
    entity.add("notes", row.pop("Remark"))
    entity.add("sourceUrl", row.pop("Links"))
    entity.add("topics", "sanction")

    if entity.schema.is_a("Person"):
        entity.add("lastName", row.pop("Lastname"))
        entity.add("firstName", row.pop("Firstname"))
        entity.add("middleName", row.pop("Middlename"))
        entity.add("gender", row.pop("Gender"))
        entity.add("birthPlace", row.pop("Birth place"))
        entity.add("country", row.pop("Birth country"))
        h.apply_dates(entity, "birthDate", row.pop("Birth date"))
        entity.add("position", row.pop("Function"))
    else:
        # There's an organization with the following Firstname
        # "صنایع شهید ستاری"
        entity.add("name", row.pop("Firstname"))
        entity.add("notes", row.pop("Function"))
        row.pop("Gender")  # Rustecdrone is male, apparently.

    sanction = h.make_sanction(context, entity)
    for listing_date in row.pop("Publication date"):
        h.apply_date(sanction, "listingDate", listing_date)
    for embargo in row.pop("Embargos"):
        program_id = h.lookup_sanction_program_key(context, embargo)
        sanction.add("programId", program_id)
    sanction.add("reason", row.pop("Regulation"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="utf-8-sig") as fh:
        reader = DictReader(fh, delimiter=";")
        for row in reader:
            # Use raw values before any splitting/replacing for entity id
            name = row.get("Wholename")
            birth_date = row.get("Birth date")
            birth_country = row.get("Birth country")
            entity_id = context.make_id(name, birth_date, birth_country)
            # They split distinct values with newlines. They also have some
            # literal backslash-n sequences which are some kind of data handling mistake
            # and not delimiting distinct values.
            row = {k: v.replace("\\n", " ") for k, v in row.items()}
            split_row = {k: h.multi_split(v, ["\n"]) for k, v in row.items()}
            crawl_row(context, entity_id, split_row)

    # TODO: Stop raising once we're through the initial bunch of reviews.
    assert_all_accepted(context, raise_on_unaccepted=True)
