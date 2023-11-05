import re
import bz2
import orjson
from functools import lru_cache
from typing import Any, Optional
from followthemoney.util import join_text, make_entity_id

from zavod import Context, Entity

SCHEMES = {
    "HRA": ("Company", "Unternehmen"),
    "HRB": ("Company", "Kapitalgesellschaft"),
    "VR": ("Organization", "Verein"),
    "PR": ("Organization", "Partnerschaft (Personengesellschaft)"),
    "GnR": ("Organization", "Genossenschaft"),
}

REL_SCHEMS = {
    "Geschäftsführer": "Directorship",
    "Inhaber": "Ownership",
    "Liquidator": "Directorship",
    "Persönlich haftender Gesellschafter": "Ownership",
    "Prokurist": "Directorship",
    "Vorstand": "Directorship",
}

MAPPING = {
    "firstname": "firstName",
    "lastname": "lastName",
    "city": "address",
    "start_date": "startDate",
    "end_date": "endDate",
    "position": "role",
    "flag": "description",
}


RE_PATTERNS = (
    re.compile(
        r"(?P<name>.*),\s(?P<city>[\w\s-]+)\s\([\w]+gericht\s(?P<reg>.+)\s(?P<reg_type>(HRA|HRB|VR|PR|GnR))\s(?P<reg_nr>[\d]+)\),?\s?(?P<summary>.*)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<name>.*)\s\([\w]+gericht\s(?P<reg>.+)\s(?P<reg_type>(HRA|HRB|VR|PR|GnR))\s(?P<reg_nr>[\d]+)\),?\s?(?P<summary>.*)",
        re.IGNORECASE,
    ),
)


@lru_cache
def parse_officer_company_name(name: str) -> dict[str, Any]:
    """
    examples:

    HA Invest GmbH, Hamburg (Amtsgericht Hamburg HRB 125617).

    VGHW Verwaltungsgesellschaft Hamburg Wandsbek mbH, Hamburg (Amtsgericht
    Hamburg HRB 139379), Die jeweiligen Geschäftsführer des persönlich
    haftenden Gesellschafters sind befugt, im Namen der Gesellschaft mit
    sich im eigenen Namen oder als Vertreter eines Dritten Rechtsgeschäfte
    abzuschließen."
    """
    for pat in RE_PATTERNS:
        m = pat.match(name)
        if m:
            return m.groupdict()
    return {}


def make_rel(
    context: Context,
    company: Entity,
    officer: Entity,
    data: dict[str, Any],
    summary: Optional[str] = None,
):
    type_ = data.pop("position")
    schema = REL_SCHEMS[type_]
    if schema == "Ownership" and company.schema.is_a("Asset"):
        proxy = context.make(schema)
        proxy.id = context.make_slug(
            "rel", make_entity_id(company.id, officer.id, type_)
        )
        proxy.add("owner", officer)
        proxy.add("asset", company)
    else:
        proxy = context.make("Directorship")
        proxy.id = context.make_slug(
            "rel", make_entity_id(company.id, officer.id, type_)
        )
        proxy.add("director", officer)
        proxy.add("organization", company)

    proxy.add("role", type_)
    proxy.add("summary", summary)
    for key, value in data.get("other_attributes", {}).items():
        if key in MAPPING:
            proxy.add(MAPPING[key], value, quiet=True)

    context.emit(proxy)


def make_officer_and_rel(context: Context, company: Entity, data: dict[str, Any]):
    type_ = data.pop("type")
    name = data.pop("name")
    rel_summary = None
    if type_ == "company":
        proxy = context.make("Company")
        parsed_data = parse_officer_company_name(name)
        if parsed_data:
            reg = (
                parsed_data.pop("reg"),
                parsed_data.pop("reg_type"),
                parsed_data.pop("reg_nr"),
            )
            proxy.id = context.make_slug(*reg)
            proxy.add("name", parsed_data.pop("name"))
            proxy.add("address", parsed_data.pop("city", None))
            proxy.add("registrationNumber", join_text(*reg))
            proxy.add("registrationNumber", join_text(*reg[1:]))

            rel_summary = parsed_data.pop("summary")
        else:
            proxy.id = context.make_slug("officer", make_entity_id(company.id, name))
            proxy.add("name", name)
    elif type_ == "person":
        proxy = context.make("Person")
        proxy.id = context.make_slug("officer", make_entity_id(company.id, name))
        proxy.add("name", name)
    else:
        context.log.warning("Unknown type: %s" % type_)
        proxy = context.make("LegalEntity")
        proxy.id = context.make_slug("officer", make_entity_id(company.id, name))
        proxy.add("name", name)

    for key, value in data.get("other_attributes", {}).items():
        if key in MAPPING:
            proxy.add(MAPPING[key], value, quiet=True)

    make_rel(context, company, proxy, data, rel_summary)
    context.emit(proxy)


def make_company(context: Context, data: dict[str, Any]) -> Entity:
    meta = data.pop("all_attributes")
    reg_art = meta.pop("_registerArt")
    reg_nr = meta.pop("native_company_number")
    schema, legalForm = SCHEMES[reg_art]
    proxy = context.make(schema)
    proxy.id = context.make_slug(reg_nr)
    proxy.add("legalForm", legalForm)
    proxy.add("registrationNumber", reg_nr)
    # FIXME? better gleif matching:
    proxy.add("registrationNumber", f'{reg_art} {meta.pop("_registerNummer")}')
    proxy.add("status", data.pop("current_status", None))
    oc_id = data.pop("company_number")
    proxy.add("opencorporatesUrl", f"https://opencorporates.com/companies/de/{oc_id}")
    proxy.add("jurisdiction", data.pop("jurisdiction_code"))
    proxy.add("name", data.pop("name"))
    proxy.add("address", data.pop("registered_address", None))
    for name in data.pop("previous_names", []):
        proxy.add("previousName", name.pop("company_name"))
    proxy.add("retrievedAt", data.pop("retrieved_at"))

    context.emit(proxy)
    return proxy


def parse_record(context: Context, record: dict[str, Any]):
    company = make_company(context, record)

    for data in record.pop("officers", []):
        make_officer_and_rel(context, company, data)


def crawl(context: Context):
    data_fp = context.fetch_resource("de_companies_ocdata.jsonl.bz2", context.data_url)
    with bz2.open(data_fp) as f:
        idx = 0
        while line := f.readline():
            record = orjson.loads(line)
            parse_record(context, record)
            idx += 1
            if idx and idx % 10_000 == 0:
                context.log.info("Parse record %d ..." % idx)
        if idx:
            context.log.info("Parsed %d records." % (idx + 1), fp=data_fp.name)
