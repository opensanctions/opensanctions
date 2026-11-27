import re

from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h
from zavod.stateful.review import assert_all_accepted


REGEX_PASSPORT = re.compile(r"^[A-Z0-9-]{6,20}$")
PASSPORT_LABELS = {"passport", "numero de passeport", "número de pasaporte"}
ADDRESS_SPLITS = [
    "Branch Office 1:",
    "Branch Office 2:",
    "Branch Office 3:",
    "Branch Office 4:",
    "Branch Office 5:",
    "Branch Office 6:",
    "Branch Office 7:",
    "Branch Office 8:",
    "Branch Office 9:",
    "Branch Office 10:",
    "Branch Office 11:",
    "Branch Office 12:",
    "Branch Office 13:",
    "Branch Office 14:",
    "Branch Office 15:",
    "Branch Office 16:",
    "iii)",
    "iv)",
    "ii)",
    "v)",
    "i)",
    "(Formerly located at)",
]


def clean_passports(context: Context, text: str) -> tuple[list[str], list[str]]:
    # Returns (passport_numbers, national_id_numbers)
    values = text.split(", ")
    passports = []
    ids = []
    is_id = None
    for value in values:
        if not value:
            continue
        if value.lower() == "national identification number":
            is_id = True
        elif value.lower() in PASSPORT_LABELS:
            is_id = False
        elif value.lower() == "unknown":
            is_id = None
        elif registry.country.clean_text(value) is not None:
            # The issuing country after a number, e.g. "Passport, R00012098, South Sudan".
            continue
        elif REGEX_PASSPORT.search(value):
            if is_id:
                ids.append(value)
            else:
                passports.append(value)
            is_id = None
        else:
            passports.append(value)
            is_id = None
    return passports, ids


def parse_aliases(names: h.Names, text: str | None) -> None:
    """Split a packed "Good, <name>, Low, <name>" alias field by its UN quality labels.

    Names can contain ", ", so each name runs up to the next label. Fields that
    do not start with a label stay whole and go to review.
    """
    if text is None:
        return
    # The capture group makes re.split keep the labels, so the parts alternate
    # between label and name, after the text before the first label:
    # "Good, A, B, Low, C" -> ["", "Good", "A, B", "Low", "C"]
    parts = re.split(r"(?:^|, )(Good|Low), ", text)
    # Text before the first label means the field does not start with one.
    if parts[0] != "":
        names.add("alias", text)
        return
    # Pair each label (odd indexes) with the name that follows it (even indexes).
    for label, name in zip(parts[1::2], parts[2::2]):
        names.add("alias" if label == "Good" else "weakAlias", name)


def crawl_row(context: Context, data: dict[str, str], key: str, checksum: str) -> None:
    full_name = data.pop("FullName", None)
    if full_name is not None:
        ent_id = data.pop("IndividualID")
        schema = "Person"
    else:
        full_name = data.pop("FirstName")
        ent_id = data.pop("EntityID")
        schema = "LegalEntity"
    entity = context.make(schema)
    entity.id = context.make_slug(ent_id, full_name)
    assert entity.id, data
    names = h.Names(name=full_name)
    entity.add("topics", "sanction")
    entity.add("notes", h.clean_note(data.pop("Comments", None)))
    if entity.schema.is_a("Person"):
        entity.add("address", data.pop("IndividualAddress", None))
        entity.add("nationality", data.pop("Nationality", None))
        entity.add("title", data.pop("Title", None))
        entity.add("position", data.pop("Designation", None))
        entity.add("birthPlace", data.pop("IndividualPlaceOfBirth", None))
        dob = data.pop("IndividualDateOfBirth", None)
        h.apply_date(entity, "birthDate", dob)
        parse_aliases(names, data.pop("IndividualAlias", None))
        passports, ids = clean_passports(context, data.pop("IndividualDocument", ""))
        entity.add("passportNumber", passports)
        entity.add("idNumber", ids)

    if entity.schema.is_a("LegalEntity"):
        address = data.pop("EntityAddress", None)
        for address in h.multi_split(address, ADDRESS_SPLITS):
            address = address.rstrip(",")
            entity.add("address", address)
        parse_aliases(names, data.pop("EntityAlias", None))

    h.apply_reviewed_names(context, entity, original=names)
    listed_on = data.pop("ListedOn", None)
    h.apply_date(entity, "createdAt", listed_on)

    # The UN reference starts with the committee's two-letter code, e.g. QDi.430.
    committee = key[:2]
    sanction = h.make_sanction(
        context,
        entity,
        source_program_key=committee,
        program_key=h.lookup_sanction_program_key(context, committee),
    )
    h.apply_date(sanction, "listingDate", listed_on)
    # ListedOn is the UN date; UN designations take effect in South Africa on adoption.
    for stmt in sanction.get_statements("listingDate"):
        sanction.adopt_statement(stmt, prop="startDate")
    sanction.add("unscId", data.pop("ReferenceNumber", None))

    context.audit_data(data, ignore=["ApplicationStatus"])
    h.finalize_sanction(context, sanction, key=key, checksum=checksum)
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.xml",
        context.data_url,
        method="POST",
        data={"fileType": "xml"},
    )
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    tables = [".//Table", ".//Table1"]
    for table in tables:
        for row in doc.findall(table):
            data = {}
            for field in row.iterchildren():
                value = field.text
                if value is None or value == "NA":
                    continue
                data[field.tag] = value
            key = data.get("ReferenceNumber")
            if key is None:
                raise ValueError(f"Row without ReferenceNumber: {data!r}")
            # IndividualID and EntityID are row numbers, which the source
            # renumbers without any change to the designations.
            checksum = h.record_hash(
                {k: v for k, v in data.items() if k not in ("IndividualID", "EntityID")}
            )
            crawl_row(context, data, key, checksum)
    potential_table = doc.find(".//Table2")
    if potential_table is not None:
        context.log.warning("Table2 found in source, but not yet implemented")
    assert_all_accepted(context, raise_on_unaccepted=False)
