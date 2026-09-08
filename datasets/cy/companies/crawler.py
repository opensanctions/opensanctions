import csv
from pathlib import Path
from collections.abc import Callable, Generator, Iterable

from followthemoney.util import join_text
from normality.cleaning import remove_unsafe_chars, squash_spaces

from zavod import Context
from zavod import helpers as h

TYPES = {"C": "HE", "P": "S", "O": "AE", "N": "BN", "B": "B"}

# The portal has been publishing the officials distribution without a download
# URL since 2026-09-07, while still serving the file it last published on
# 2026-07-31. Fall back to that file for as long as the feed omits the URL.
OFFICIALS_FALLBACK_URL = (
    "https://data.gov.cy/sites/default/files/organisation_officials_86.csv"
)


def company_id(org_type: str, reg_nr: str | None) -> str | None:
    if reg_nr is None:
        return None
    org_type_oc = TYPES.get(org_type)
    if org_type_oc is None:
        return None
    return f"oc-companies-cy-{org_type_oc}{reg_nr}".lower()


def is_organisation_record(values: list[str]) -> bool:
    """Check whether a line of the organisations CSV starts a record."""
    return values[2] in TYPES


def carried_values(values: list[str]) -> list[str]:
    """Drop the empty fields a record fragment is padded with."""
    end = len(values)
    while end > 0 and len(values[end - 1]) == 0:
        end -= 1
    return values[:end]


def join_fragments(width: int, fragments: list[list[str]]) -> list[str] | None:
    """Re-join a record the publisher split across several lines.

    Each fragment carries a run of the record's values, padded with empty fields
    to the width of the header. The last fragment ends at the last column of the
    record, so any columns the fragments don't account for are empty ones in the
    middle of the record. Returns None if the fragments carry more values than
    the record has columns, i.e. if they are not fragments of a single record.
    """
    head: list[str] = []
    for fragment in fragments[:-1]:
        head.extend(carried_values(fragment))
    tail = carried_values(fragments[-1])
    gap = width - len(head) - len(tail)
    if gap < 0:
        return None
    return head + [""] * gap + tail


def clean_row(header: list[str], values: list[str]) -> dict[str, str]:
    data: dict[str, str] = {}
    for key, value in zip(header, values):
        cleaned = squash_spaces(remove_unsafe_chars(value))
        if len(cleaned) > 0:
            data[key] = cleaned
    return data


def make_row(
    context: Context,
    header: list[str],
    fragments: list[list[str]],
    is_record: Callable[[list[str]], bool],
) -> dict[str, str] | None:
    if len(fragments) == 1:
        return clean_row(header, fragments[0])
    values = join_fragments(len(header), fragments)
    if values is None or not is_record(values):
        context.log.error("Cannot re-join split record", fragments=fragments)
        return None
    return clean_row(header, values)


def iter_rows(
    context: Context,
    path: Path,
    is_record: Callable[[list[str]], bool] | None = None,
) -> Generator[dict[str, str], None, None]:
    """Read a source CSV into dicts of the values that are set.

    The publisher sometimes breaks a record across several lines, padding each
    fragment with empty fields to the width of the header. When `is_record` is
    given, lines which don't start a record are re-joined with the fragments
    preceding them.
    """
    with open(path) as fh:
        fh.read(1)  # bom
        reader = csv.reader(fh)
        header = next(reader)
        width = len(header)
        fragments: list[list[str]] = []
        carried = 0
        for values in reader:
            if len(values) == 0:  # blank line
                continue
            if len(values) != width:
                context.log.error("Unexpected column count", values=values)
                continue
            if is_record is None:
                yield clean_row(header, values)
                continue
            # A line which starts a record, or which carries more values than
            # are left in the record being assembled, ends the previous record.
            if len(fragments) > 0 and (
                is_record(values) or carried + len(carried_values(values)) > width
            ):
                row = make_row(context, header, fragments, is_record)
                if row is not None:
                    yield row
                fragments, carried = [], 0
            fragments.append(values)
            carried += len(carried_values(values))
        if is_record is not None and len(fragments) > 0:
            row = make_row(context, header, fragments, is_record)
            if row is not None:
                yield row


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


def get_path(file_paths: dict[str, Path], prefix: str) -> Path | None:
    matched_files = [
        path for name, path in file_paths.items() if name.startswith(prefix)
    ]
    assert len(matched_files) <= 1, (prefix, len(matched_files))
    if len(matched_files) == 0:
        return None
    return matched_files[0]


def require_path(file_paths: dict[str, Path], prefix: str) -> Path:
    path = get_path(file_paths, prefix)
    if path is None:
        raise RuntimeError(f"Source feed has no distribution for: {prefix}")
    return path


def fetch_files(context: Context) -> dict[str, Path]:
    headers = {"Accept": "application/json"}
    meta = context.fetch_json(context.data_url, headers=headers)

    files: dict[str, Path] = {}
    for dist in meta["dcat:Distribution"]:
        download = dist.get("dcat:downloadURL")
        if download is None:
            # The portal keeps listing a distribution while the file it points
            # at is missing, e.g. between two publications of the data.
            context.log.warning(
                "Distribution has no download URL", title=dist.get("dct:title")
            )
            continue
        dist_url = download["@rdf:resource"]
        file_name = dist_url.rsplit("/")[-1]
        files[file_name] = context.fetch_resource(file_name, dist_url)
    return files


def crawl(context: Context) -> None:
    files = fetch_files(context)

    office_path = require_path(files, "registered_office_")
    addresses = load_addresses(iter_rows(context, office_path))
    context.log.info(f"Loaded {len(addresses)} addresses")

    org_path = require_path(files, "organisations_")
    rows = iter_rows(context, org_path, is_organisation_record)
    parse_organisations(context, rows, addresses)

    officials_path = get_path(files, "organisation_officials_")
    if officials_path is None:
        context.log.warning(
            "Officials file is missing from the source feed, using the last "
            "one the portal published",
            url=OFFICIALS_FALLBACK_URL,
        )
        file_name = OFFICIALS_FALLBACK_URL.rsplit("/")[-1]
        officials_path = context.fetch_resource(file_name, OFFICIALS_FALLBACK_URL)
    parse_officials(context, iter_rows(context, officials_path))
