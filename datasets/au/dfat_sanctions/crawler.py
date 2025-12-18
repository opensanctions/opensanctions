import string
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import openpyxl
from normality import slugify
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h

SPLITS = [" %s)" % char for char in string.ascii_lowercase]
ADDRESS_SPLITS = [
    ";",
    "ii) ",
    "iii) ",
    "a) ",
    "b) ",
    "c) ",
    "d) ",
    "_x000D_,",
    "_x000D_\n",
    "_x000D_",
]
PROVISION_FIELDS = [
    "travel_ban",
    "arms_embargo",
    "maritime_restriction",
    "targeted_financial_sanction",
]

# Each entry carries a reference and other names are listed as references with a letter attached
# e.g. 101 for the primary name and 101a for the alias.
# Usually, the names are listed in contiguous blocks, but sometimes they're not
# (they're interrupted by other entries).


def clean_date(date: str) -> list[str]:
    splits = [
        "Approximately",
        ", and,",
        " and ",
        "g), ",
        "h), ",
        "i), ",
        "j), ",
        "g) ",
        "h) ",
        "i) ",
        "j) ",
        ", ",
        " ,",
        ",\xa0",
        "\xa0",
        " ",
    ]
    dates = []
    date_str = str(date).lower()
    if " to " in date_str or "between" in date_str:
        return [date_str]
    if isinstance(date, datetime):
        date = date.date().isoformat()
    if isinstance(date, int):
        date = str(date)
    if date is None:
        return dates
    date = date.replace("\n", " ")
    for part in h.multi_split(date, splits):
        part = part.strip().strip(",")
        if not len(part):
            continue
        dates.append(part)
    return dates


def clean_reference(ref: str) -> Optional[str]:
    """Given a reference like 101a, return 101"""
    number = ref
    while len(number):
        try:
            return str(int(number))
        except Exception:
            number = number[:-1]
    raise ValueError()


def parse_reference(
    context: Context, reference: str, rows: list[dict[str, Any]]
) -> None:
    schemata = set()
    for row in rows:
        type_ = row.pop("type")
        # Lookup by reference first to allow an override for references where there
        # are two conflicting type specs.
        schema = context.lookup_value("type", reference)
        if schema is None:
            schema = context.lookup_value("type", type_)
        if schema is None:
            context.log.warning("Unknown entity type", type=type_, reference=reference)
            return
        schemata.add(schema)
    if len(schemata) > 1:
        context.log.error(
            "Multiple entity types",
            schemata=list(schemata),
            reference=reference,
            # rows=rows,
        )
        return
    entity = context.make(schemata.pop())

    primary_name: Optional[str] = None
    names: list[tuple[str, str]] = []
    for row in rows:
        name = row.pop("name_of_individual_or_entity")
        name_type = row.pop("name_type")
        name_prop = context.lookup_value("name_type", name_type)
        if row.pop("alias_strength") == "Weak":
            name_prop = "weakAlias"
        if name_prop is None:
            context.log.warning("Unknown name type", name_type=name_type)
            return
        names.append((name_prop, name))
        if name_prop == "name" or primary_name is None:
            primary_name = name

    entity.id = context.make_slug(reference, primary_name)
    for name_prop, name in names:
        entity.add(name_prop, name)

    primary_name = None
    for row in rows:
        addr = row.pop("address")
        if addr is not None:
            for part in h.multi_split(addr, SPLITS):
                for sub_part in h.multi_split(part, ADDRESS_SPLITS):
                    address = h.make_address(context, full=sub_part)
                    h.apply_address(context, entity, address)
        source_program = row.pop("committees")
        source_program = source_program.strip() if source_program else None
        sanction = h.make_sanction(
            context,
            entity,
            key=source_program,
            program_name=source_program,
            source_program_key=source_program,
            program_key=(
                h.lookup_sanction_program_key(context, source_program)
                if source_program
                else None
            ),
        )
        country = h.multi_split(row.pop("citizenship"), [","])
        if entity.schema.is_a("Person"):
            entity.add("citizenship", country)
        else:
            entity.add("country", country)
        if entity.schema.properties.get("imoNumber"):
            entity.add("imoNumber", row.pop("imo_number"))
        dates = clean_date(row.pop("date_of_birth"))
        h.apply_dates(entity, "birthDate", dates)
        pob = row.pop("place_of_birth")
        entity.add("birthPlace", h.multi_split(pob, SPLITS + ["a) "]), quiet=True)
        notes = h.clean_note(row.pop("additional_information"))
        if notes:
            entity.add("notes", (note.replace("_x000D_", "") for note in notes))
        listing_info = row.pop("listing_information")
        if isinstance(listing_info, datetime):
            h.apply_date(entity, "createdAt", listing_info)
            sanction.add("listingDate", listing_info)
        else:
            sanction.add("summary", listing_info.replace("_x000D_", ""))
        designation_instrument = row.pop("instrument_of_designation")
        # designation instrument is very often the same as listing info
        if designation_instrument and designation_instrument != listing_info:
            sanction.add("summary", designation_instrument)
        # TODO: consider parsing if it's not a datetime?
        for field in PROVISION_FIELDS:
            if row.pop(field):  # Boolean field indicating if the sanction applies
                # Convert the field name into a readable label, e.g. "arms_embargo" → "Arms embargo"
                sanction.add("provisions", field.replace("_", " ").capitalize())
        control_date = row.pop("control_date")
        h.apply_date(sanction, "startDate", control_date)
        h.apply_date(entity, "createdAt", control_date)
        context.audit_data(
            row,
            ignore=["reference"],
        )

    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    references = defaultdict(list)
    raw_references: set[str] = set()
    reference_blocks_seen_count: dict[str, int] = dict()
    last_clean_ref = None
    for sheet in workbook.worksheets:
        headers: Optional[list[str]] = None
        for row_num, raw_row in enumerate(sheet.rows):
            cells = [c.value for c in raw_row]
            if headers is None:
                headers = [slugify(h, sep="_") or "" for h in cells]
                continue
            row = dict(zip(headers, cells))

            # We use the numeric part of the reference to combine rows about the same entity.
            raw_ref = row.get("reference")
            context.log.debug("Parsing row", row_num=row, raw_ref=raw_ref)
            if raw_ref is None:
                row_values = {
                    str(v).strip() or None for v in row.values() if v is not None
                }
                if row_values and row_values != {None}:
                    context.log.warning("No reference", row=row)
                continue
            raw_ref = str(raw_ref)
            # Get the reference number without the letter suffix.
            reference = clean_reference(raw_ref)
            if reference is None:
                context.log.warning("Couldn't clean raw reference", ref=raw_ref)
                continue

            # If we've seen this reference before, add a suffix to each new
            # non-contiguous block. We've seen IDs reused (by mistake) for
            # unrelated entities on non-contiguous rows.
            # For example, if there is the following sequence:
            # - 101
            # - 102
            # - 101a
            # We want to manually check that 101 and 101a are the same entity.

            # first row of contiguous block of clean reference
            if last_clean_ref != reference:
                reference_seen_count = reference_blocks_seen_count.get(reference, 0) + 1
                reference_blocks_seen_count[reference] = reference_seen_count
            # Stash clean ref before adding suffix
            # to simplify detecting contiguous blocks
            last_clean_ref = reference

            # Add suffix so that this block is treated as distinct block from
            # earlier iterations of this reference
            if reference_seen_count > 1:
                context.log.warning(
                    f"Already seen a reference block before for {reference}. Adding iteration suffix, check if {raw_ref} actually belongs to {reference}",
                    ref=reference,
                    raw_ref=raw_ref,
                    reference_seen_count=reference_seen_count,
                )
                reference = f"{reference}-{reference_seen_count}"
                raw_ref = f"{raw_ref}-{reference_seen_count}"

            # Sanity check that this raw reference isn't duplicated within its clean ref block.
            if raw_ref in raw_references:
                context.log.warning("Duplicate reference", raw_ref=raw_ref)
            raw_references.add(raw_ref)

            references[reference].append(row)

    for ref, rows in references.items():
        parse_reference(context, ref, rows)
