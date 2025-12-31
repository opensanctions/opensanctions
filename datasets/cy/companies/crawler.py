import csv
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional

from followthemoney.util import join_text
from normality.cleaning import remove_unsafe_chars, squash_spaces

from zavod import Context
from zavod import helpers as h

TYPES = {"C": "HE", "P": "S", "O": "AE", "N": "BN", "B": "B"}


def company_id(org_type: str, reg_nr: Optional[str]) -> Optional[str]:
    if reg_nr is None:
        return None
    org_type_oc = TYPES.get(org_type)
    if org_type_oc is None:
        return None
    return f"oc-companies-cy-{org_type_oc}{reg_nr}".lower()


def iter_rows(path: Path) -> Generator[Dict[str, str], None, None]:
    with open(path, "r") as fh:
        fh.read(1)  # bom
        for row in csv.DictReader(fh):
            data = {}
            for k, v in row.items():
                sv = squash_spaces(remove_unsafe_chars(v))
                if len(sv) > 0:
                    data[k] = sv
            yield data


def parse_organisations(
    context: Context, rows: Iterable[Dict[str, str]], addresses: Dict[str, str]
) -> None:
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE", None)
        reg_nr = row.pop("REGISTRATION_NO", None)
        if org_type in (None, "Εμπορική Επωνυμία"):
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
        entity.add("status", row.pop("ORGANISATION_STATUS"))
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


def parse_officials(context: Context, rows: Iterable[Dict[str, str]]) -> None:
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


def load_addresses(rows: Iterable[Dict[str, str]]) -> Dict[str, str]:
    addresses: Dict[str, str] = {}
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


def get_path(file_paths: Dict[str, Path], prefix: str) -> Path:
    matched_files = [
        path for name, path in file_paths.items() if name.startswith(prefix)
    ]
    assert len(matched_files) == 1, (prefix, len(matched_files))
    return matched_files[0]


def crawl(context: Context) -> None:
    headers = {"Accept": "application/json"}
    meta = context.fetch_json(context.data_url, headers=headers)

    files: Dict[str, Path] = {}
    for dist in meta["dcat:Distribution"]:
        dist_url = dist["dcat:downloadURL"]["@rdf:resource"]
        file_name = dist_url.rsplit("/")[-1]
        file_path = context.fetch_resource(file_name, dist_url)
        files[file_name] = file_path

    office_path = get_path(files, "registered_office_")
    addresses = load_addresses(iter_rows(office_path))
    context.log.info("Loaded %d addresses" % len(addresses))

    org_path = get_path(files, "organisations_")
    parse_organisations(context, iter_rows(org_path), addresses)

    officials_path = get_path(files, "organisation_officials_")
    parse_officials(context, iter_rows(officials_path))
