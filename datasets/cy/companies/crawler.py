import csv
from pathlib import Path
from typing import Dict, Generator, Iterable, Optional
from datetime import datetime
from normality import collapse_spaces
from followthemoney.util import join_text

from zavod import Context

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"

TYPES = {"C": "HE", "P": "S", "O": "AE", "N": "BN", "B": "B"}


def parse_date(text: Optional[str]) -> Optional[str]:
    if text is None or not len(text.strip()):
        return None
    return datetime.strptime(text, "%d/%m/%Y").date().isoformat()


def company_id(org_type: str, reg_nr: str) -> Optional[str]:
    org_type_oc = TYPES.get(org_type)
    if org_type_oc is None:
        return None
    return f"oc-companies-cy-{org_type_oc}{reg_nr}".lower()


def iter_rows(path: Path) -> Generator[Dict[str, str], None, None]:
    with open(path, "r") as fh:
        fh.read(1)  # bom
        for row in csv.DictReader(fh):
            yield row


def parse_organisations(
    context: Context, rows: Iterable[Dict[str, str]], addresses: Dict[str, str]
) -> None:
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE")
        reg_nr = row.pop("REGISTRATION_NO")
        if org_type in ("", "Εμπορική Επωνυμία"):
            continue
        entity = context.make("Company")
        entity.id = company_id(org_type, reg_nr)
        if entity.id is None:
            context.log.error("Could not make ID", org_type=org_type, reg_nr=reg_nr)
            continue
        entity.add("name", row.pop("ORGANISATION_NAME"))
        entity.add("status", row.pop("ORGANISATION_STATUS"))
        if org_type == "O":
            entity.add("country", "cy")
        else:
            entity.add("jurisdiction", "cy")
        org_type_oc = TYPES[org_type]
        oc_id = f"{org_type_oc}{reg_nr}"
        oc_url = f"https://opencorporates.com/companies/cy/{oc_id}"
        entity.add("opencorporatesUrl", oc_url)
        entity.add("registrationNumber", oc_id)
        entity.add("registrationNumber", f"{org_type}{reg_nr}")
        org_type_text = row.pop("ORGANISATION_TYPE")
        org_subtype = row.pop("ORGANISATION_SUB_TYPE")
        if len(org_subtype.strip()):
            org_type_text = f"{org_type_text} - {org_subtype}"
        entity.add("legalForm", org_type_text)
        reg_date = parse_date(row.pop("REGISTRATION_DATE"))
        entity.add("incorporationDate", reg_date)
        status_date = parse_date(row.pop("ORGANISATION_STATUS_DATE"))
        entity.add("modifiedAt", status_date)

        addr_id = row.pop("ADDRESS_SEQ_NO")
        entity.add("address", addresses.get(addr_id))
        context.emit(entity)
        # print(entity.to_dict())
        context.audit_data(row, ignore=["NAME_STATUS_CODE", "NAME_STATUS"])


def parse_officials(context: Context, rows: Iterable[Dict[str, str]]) -> None:
    org_types = list(TYPES.keys())
    for row in rows:
        org_type = row.pop("ORGANISATION_TYPE_CODE")
        if org_type not in org_types:
            continue
        reg_nr = row.pop("REGISTRATION_NO")
        name = row.pop("PERSON_OR_ORGANISATION_NAME")
        position = row.pop("OFFICIAL_POSITION")
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
        street = row.pop("STREET")
        building = row.pop("BUILDING")
        territory = row.pop("TERRITORY")
        address = join_text(building, street, territory, sep=", ")
        if address is not None:
            address = collapse_spaces(address.replace("_", ""))
            if address is not None:
                addresses[seq_no] = address
    return addresses


def crawl(context: Context) -> None:
    headers = {"Accept": "application/json", "User-Agent": UA}
    meta = context.fetch_json(context.data_url, headers=headers)

    files: Dict[str, Path] = {}
    for dist in meta['dcat:Distribution']:
        dist_url = dist['dcat:downloadURL']['@rdf:resource']
        file_name = dist_url.rsplit('/')[-1]
        file_path = context.fetch_resource(file_name, dist_url)
        files[file_name] = file_path
    
    for name, path in files.items():
        if name.startswith("registered_office_"):
            addresses = load_addresses(iter_rows(path))
    
    for name, path in files.items():
        if name.startswith("organisations_"):
            rows = iter_rows(path)
            parse_organisations(context, rows, addresses)
        if name.startswith("organisation_officials_"):
            rows = iter_rows(path)
            parse_officials(context, rows)
    