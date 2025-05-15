import string
import openpyxl
from typing import List, Optional, Set, Tuple, Dict, Any
from collections import defaultdict
from normality import slugify
from datetime import datetime
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h

SPLITS = [" %s)" % char for char in string.ascii_lowercase]
ADDRESS_SPLITS = [";", "ii) ", "iii) "]
# We don't want to duplicate UNSC resolutions on the website.
IGNORE_PROGRAM = [
    "1267/1989/2253 (ISIL (Da'esh) and Al-Qaida)",
    "1373 (2001)",
    "1518 (Iraq)",
    "1533 (Democratic Republic of the Congo)",
    "1591 (Sudan)",
    "1718 (DPRK)",
    "1970 (Libya) 1973 (Libya)",
    "1970 (Libya)",
    "1973 (Libya)",
    "1988 (Taliban)",
    "2093 (2013)",
    "2127 (Central African Republic)",
    "2140 (Yemen)",
    "2206 (South Sudan)",
    "2713 (Al-Shabaab)",
    "751 (Somalia and Eritrea)",
]


def clean_date(date):
    splits = [
        "a)",
        "b)",
        "c)",
        "d)",
        "e)",
        "f)",
        "g)",
        "h)",
        "i)",
        " or ",
        " and ",
        " to ",
        "alt DOB:",
        "alt DOB",
        ";",
        ">>",
        "Approximately",
        "approximately",
        "Approx",
        "Between",
        "circa",
        "Circa",
    ]
    dates = []
    if isinstance(date, datetime):
        date = date.date().isoformat()
    if isinstance(date, int):
        date = str(date)
    date = h.remove_bracketed(date)
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
    number = ref
    while len(number):
        try:
            return str(int(number))
        except Exception:
            number = number[:-1]
    raise ValueError()


def clean_country(country: str) -> List[str]:
    return h.multi_split(country, ["a)", "b)", "c)", "d)", ";", ",", " and "])


def parse_reference(
    context: Context, reference: int, rows: List[Dict[str, Any]]
) -> None:
    schemata = set()
    for row in rows:
        type_ = row.pop("type")
        schema = context.lookup_value("type", type_)
        if schema is None:
            schema = context.lookup_value("type", reference)
            if schema is None:
                context.log.warning(
                    "Unknown entity type", type=type_, reference=reference
                )
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
    names: List[Tuple[str, str]] = []
    for row in rows:
        name = row.pop("name_of_individual_or_entity", None)
        name_type = row.pop("name_type")
        name_prop = context.lookup_value("name_type", name_type)
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
            program_key=h.lookup_sanction_program_key(
                context, source_program, ignore_source_key=IGNORE_PROGRAM
            ),
        )
        country = clean_country(row.pop("citizenship"))
        if entity.schema.is_a("Person"):
            entity.add("nationality", country)
        else:
            entity.add("country", country)
        dates = clean_date(row.pop("date_of_birth"))
        if dates != ["na"]:
            h.apply_dates(entity, "birthDate", dates)
        entity.add("birthPlace", row.pop("place_of_birth"), quiet=True)
        entity.add("notes", h.clean_note(row.pop("additional_information")))
        listing_info = row.pop("listing_information")
        if isinstance(listing_info, datetime):
            entity.add("createdAt", listing_info)
            sanction.add("listingDate", listing_info)
        else:
            sanction.add("summary", listing_info)
        # TODO: consider parsing if it's not a datetime?

        control_date = row.pop("control_date")
        sanction.add("startDate", control_date)
        entity.add("createdAt", control_date)
        context.audit_data(row, ignore=["reference"])

    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    references = defaultdict(list)
    raw_references: Set[str] = set()
    reference_iteration: Dict[str, int] = dict()
    last_clean_ref = None
    for sheet in workbook.worksheets:
        headers: Optional[List[str]] = None
        for row_num, row in enumerate(sheet.rows):
            cells = [c.value for c in row]
            if headers is None:
                headers = [slugify(h, sep="_") for h in cells]
                continue
            row = dict(zip(headers, cells))

            # We use the numeric part of the reference to combine rows about the same entity.
            raw_ref = row.get("reference")
            context.log.debug("Parsing row", row_num=row, raw_ref=raw_ref)
            if raw_ref is None:
                row_values = {v.strip() or None for v in row.values() if v is not None}
                if row_values and row_values != {None}:
                    context.log.warning("No reference", row=row)
                continue
            raw_ref = str(raw_ref)
            # get clean ref
            reference = clean_reference(raw_ref)
            if reference is None:
                context.log.warning("Couldn't clean raw reference", ref=raw_ref)
                continue

            iteration = reference_iteration.get(reference, None)

            # If we've seen this reference before, add a suffix to each new
            # non-contiguous block. We've seen IDs reused (by mistake) for
            # unrelated entities on non-contiguous rows.

            # first row of contiguous block of clean reference
            if last_clean_ref != reference:
                iteration = (iteration or 0) + 1
                reference_iteration[reference] = iteration
            # Stash clean ref before adding suffix
            # to simplify detecting contiguous blocks
            last_clean_ref = reference

            # Add suffix so that this block is treated as distinct block from
            # earlier iterations of this reference
            if iteration > 1 and raw_ref != "2940j":
                context.log.warning(
                    "Already seen reference. Adding iteration suffix.",
                    ref=reference,
                    iteration=iteration,
                )
                reference = f"{reference}-{iteration}"
                raw_ref = f"{raw_ref}-{iteration}"

            # Sanity check that this raw reference isn't duplicated within its clean ref block.
            if raw_ref in raw_references and raw_ref != "8058":
                raise ValueError("Duplicate reference: %s" % raw_ref)
            raw_references.add(raw_ref)

            references[reference].append(row)

    for ref, rows in references.items():
        parse_reference(context, ref, rows)
