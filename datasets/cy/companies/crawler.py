import csv
from pathlib import Path
from collections.abc import Generator, Iterable

from followthemoney.util import join_text
from normality.cleaning import remove_unsafe_chars, squash_spaces

from zavod import Context
from zavod import helpers as h

TYPES = {"C": "HE", "P": "S", "O": "AE", "N": "BN", "B": "B"}
NAME_COL = "ORGANISATION_NAME"
TYPE_COL = "ORGANISATION_TYPE_CODE"


def company_id(org_type: str, reg_nr: str | None) -> str | None:
    if reg_nr is None:
        return None
    org_type_oc = TYPES.get(org_type)
    if org_type_oc is None:
        return None
    return f"oc-companies-cy-{org_type_oc}{reg_nr}".lower()


def iter_cells(path: Path) -> Generator[list[str], None, None]:
    """Yield the header line, then every non-empty line, as cleaned cells."""
    with open(path) as fh:
        fh.read(1)  # bom
        for cells in csv.reader(fh):
            if len(cells) == 0:
                continue
            yield [squash_spaces(remove_unsafe_chars(c)) for c in cells]


def make_row(header: list[str], cells: list[str]) -> dict[str, str]:
    """Map cells onto their column names, dropping columns without a value."""
    return {k: v for k, v in zip(header, cells) if len(v) > 0}


def iter_rows(path: Path) -> Generator[dict[str, str], None, None]:
    cells = iter_cells(path)
    header = next(cells)
    for row in cells:
        yield make_row(header, row)


def rejoin_record(
    context: Context, header: list[str], name: str, cells: list[str]
) -> dict[str, str] | None:
    """Map the trailing half of a split record back onto its columns.

    The trailing values are right-aligned against the end of the record, i.e.
    they are padded with empty columns on the right to make up a full-width
    line. Stripping that padding recovers which column each value belongs to.
    """
    tail = list(cells)
    while len(tail) > 0 and len(tail[-1]) == 0:
        tail.pop()
    pad = len(header) - len(tail) - 1
    if pad < 0:
        context.log.error("Cannot re-join split record", name=name, cells=cells)
        return None
    row = make_row(header, [name] + [""] * pad + tail)
    if row.get(TYPE_COL) not in TYPES:
        context.log.error("Cannot re-join split record", name=name, cells=cells)
        return None
    context.log.info("Re-joined split record", row=row)
    return row


def iter_organisations(
    context: Context, path: Path
) -> Generator[dict[str, str], None, None]:
    """Yield organisation rows, re-joining records the source split across lines.

    The registry occasionally breaks a record right after the organisation
    name: the name is published on a line of its own and the rest of the record
    follows on a later line. Both halves are padded out to the full column
    count, so they parse as syntactically valid rows and can only be recognised
    by their contents.
    """
    cells = iter_cells(path)
    header = next(cells)
    # The re-joining below only holds while the name is the leading column.
    assert header[0] == NAME_COL, header
    pending: str | None = None
    for row in cells:
        if not any(len(c) > 0 for c in row):
            continue
        fields = make_row(header, row)
        if list(fields.keys()) == [NAME_COL]:
            # A line carrying nothing but a name: the values that belong with
            # it are published on one of the following lines.
            if pending is not None:
                context.log.warning("Incomplete split record", name=pending)
            pending = fields[NAME_COL]
            continue
        if pending is not None:
            merged = rejoin_record(context, header, pending, row)
            pending = None
            if merged is not None:
                yield merged
            continue
        yield fields
    if pending is not None:
        context.log.warning("Incomplete split record", name=pending)


def parse_organisations(
    context: Context, rows: Iterable[dict[str, str]], addresses: dict[str, str]
) -> None:
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE", None)
        reg_nr = row.pop("REGISTRATION_NO", None)
        if org_type is None or org_type == "Εμπορική Επωνυμία":
            continue
        if reg_nr is None:
            continue
        entity = context.make("Company")
        entity.id = company_id(org_type, reg_nr)
        if entity.id is None:
            context.log.error(
                "Could not make ID", org_type=org_type, reg_nr=reg_nr, row=row
            )
            continue
        entity.add("name", row.pop("ORGANISATION_NAME"), lang="mul")
        entity.add("status", row.pop("ORGANISATION_STATUS", None))
        if org_type == "O":
            entity.add("country", "cy")
        else:
            entity.add("jurisdiction", "cy")
        entity.add("registrationNumber", f"{org_type} {reg_nr}")
        org_type_oc = TYPES[org_type]
        if org_type_oc not in (None, "B"):
            oc_id = f"{org_type_oc}{reg_nr}"
            oc_url = f"https://opencorporates.com/companies/cy/{oc_id}"
            entity.add("opencorporatesUrl", oc_url)
            entity.add("registrationNumber", oc_id)
        org_type_text = row.pop("ORGANISATION_TYPE")
        org_subtype = row.pop("ORGANISATION_SUB_TYPE", None)
        if org_subtype is not None and len(org_subtype.strip()):
            org_type_text = f"{org_type_text} - {org_subtype}"
        entity.add("legalForm", org_type_text)
        h.apply_date(entity, "incorporationDate", row.pop("REGISTRATION_DATE"))
        h.apply_date(entity, "modifiedAt", row.pop("ORGANISATION_STATUS_DATE", None))

        addr_id = row.pop("ADDRESS_SEQ_NO", None)
        if addr_id is not None:
            entity.add("address", addresses.get(addr_id))
        context.emit(entity)
        # print(entity.to_dict())
        context.audit_data(row, ignore=["NAME_STATUS_CODE", "NAME_STATUS"])


def parse_officials(context: Context, rows: Iterable[dict[str, str]]) -> None:
    org_types = list(TYPES.keys())
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE", None)
        if org_type not in org_types:
            continue
        reg_nr = row.pop("REGISTRATION_NO", None)
        name = row.pop("PERSON_OR_ORGANISATION_NAME", None)
        if name is None:
            continue
        position = row.pop("OFFICIAL_POSITION", None)
        entity = context.make("LegalEntity")
        entity.id = context.make_id(org_type, reg_nr, name)
        entity.add("name", name)
        if not entity.has("name"):
            # The name was rejected (e.g. a phone number in the name column), so
            # there is nothing to emit - skip the official and its directorship.
            continue
        context.emit(entity)

        link = context.make("Directorship")
        link.id = context.make_id("Directorship", org_type, reg_nr, name, position)
        org_id = company_id(org_type, reg_nr)
        if org_id is None:
            context.log.error("Could not make ID", org_type=org_type, reg_nr=reg_nr)
            continue
        link.add("organization", org_id)
        link.add("director", entity.id)
        link.add("role", position)
        context.emit(link)


def load_addresses(rows: Iterable[dict[str, str]]) -> dict[str, str]:
    addresses: dict[str, str] = {}
    for row in rows:
        seq_no = row.pop("ADDRESS_SEQ_NO")
        if seq_no is None:
            continue
        street = row.pop("STREET", None)
        building = row.pop("BUILDING", None)
        territory = row.pop("TERRITORY", None)
        address = join_text(building, street, territory, sep=", ")
        if address is not None:
            address = address.replace(",,", ",")
            address = address.replace("_", "")
            address = squash_spaces(address)
            if address is not None:
                addresses[seq_no] = address
    return addresses


def get_path(file_paths: dict[str, Path], prefix: str) -> Path:
    matched_files = [
        path for name, path in file_paths.items() if name.startswith(prefix)
    ]
    assert len(matched_files) == 1, (prefix, len(matched_files))
    return matched_files[0]


def crawl(context: Context) -> None:
    headers = {"Accept": "application/json"}
    meta = context.fetch_json(context.data_url, headers=headers)

    files: dict[str, Path] = {}
    for dist in meta["dcat:Distribution"]:
        dist_url = dist["dcat:downloadURL"]["@rdf:resource"]
        file_name = dist_url.rsplit("/")[-1]
        file_path = context.fetch_resource(file_name, dist_url)
        files[file_name] = file_path

    office_path = get_path(files, "registered_office_")
    addresses = load_addresses(iter_rows(office_path))
    context.log.info(f"Loaded {len(addresses)} addresses")

    org_path = get_path(files, "organisations_")
    parse_organisations(context, iter_organisations(context, org_path), addresses)

    officials_path = get_path(files, "organisation_officials_")
    parse_officials(context, iter_rows(officials_path))
