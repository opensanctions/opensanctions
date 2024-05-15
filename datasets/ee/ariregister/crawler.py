import ijson  # type: ignore
import zipfile
from datetime import datetime
from typing import Any, Generator, Optional, Tuple, Dict, TypeVar, Union
from followthemoney.util import make_entity_id

from zavod import Context, Entity
from zavod import helpers as h

Item = Dict[str, Any]
Default = TypeVar("Default")

SOURCES = {
    "officers1": "ettevotja_rekvisiidid__kaardile_kantud_isikud.json",
    "officers2": "ettevotja_rekvisiidid__kandevalised_isikud.json",
}
PATTERN = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/%s"

TYPES = {
    "Füüsilisest isikust ettevõtja": "Person",  # Self-employed person
    "Kohaliku omavalitsuse asutus": "PublicBody",  # Local government body
    "Mittetulundusühing": "Organization",  # Non-profit association
    # Executive or other public institution:
    "Täidesaatva riigivõimu asutus või riigi muu institutsioon": "PublicBody",
    "Sihtasutus": "Organization",  # Foundation
}


def get_value(
    data: Item, keys: Tuple[str, ...], default: Optional[Default] = None
) -> Union[Default, Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


def parse_date(value: Optional[str] = None) -> Optional[str]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d.%m.%Y").date().isoformat()
    except ValueError:
        return None


def get_address(data: Item) -> Optional[str]:
    value = data.pop("aadress_ads__ads_normaliseeritud_taisaadress", None)
    if value is not None:
        return value
    parts = []
    for key in ("aadress_ehak_tekstina", "aadress_tanav_maja_korter"):
        value = data.pop(key, None)
        if value is not None:
            parts.append(value)
    if parts:
        return ", ".join(parts).strip(", ")
    return None


def make_proxy(context: Context, row: Item, schema: Optional[str] = None) -> Entity:
    if schema is None:
        schema = "LegalEntity"
    proxy = context.make(schema)
    ident = row.pop("ariregistri_kood")
    proxy.id = context.make_slug(ident)
    proxy.add("registrationNumber", ident)
    proxy.add("name", row.pop("nimi"))
    proxy.add("sourceUrl", f"https://ariregister.rik.ee/eng/company/{ident}")
    proxy.add("jurisdiction", "ee")
    return proxy


def make_officer(context: Context, data: Item, company_id: str) -> Entity:
    legal_form = data.pop("isiku_tyyp", None)
    id_number = get_value(data, ("isikukood_registrikood", "isikukood"))
    first_name, last_name = data.pop("eesnimi", None), get_value(
        data, ("nimi_arinimi", "nimi")
    )
    proxy = context.make("LegalEntity")
    address = get_address(data)
    proxy.id = context.make_slug(id_number)
    if proxy.id is None:
        ident_id = make_entity_id(address)
        if ident_id is None:
            ident_id = id_number or company_id
        name = (
            f"{first_name} {last_name}"
            if first_name and last_name
            else first_name or last_name
        )
        proxy.id = context.make_slug("officer", name, ident_id)

    if legal_form == "F":
        proxy.add_schema("Person")
        proxy.add("idNumber", id_number)
        h.apply_name(proxy, first_name=first_name, last_name=last_name)
    else:
        proxy.add("name", " ".join((first_name or "", last_name or "")).strip())
        proxy.add("registrationNumber", id_number)

    proxy.add("country", data.pop("aadress_riik"))
    email = data.pop("email", None)
    if email is not None:
        email = email.rstrip(".")
        proxy.add("email", email)
    proxy.add("address", address)
    return proxy


def make_rel(
    context: Context,
    company: Entity,
    officer: Entity,
    schema: str,
    data: Item,
    role: Optional[str] = None,
) -> Entity:
    rel = context.make(schema)
    rel.id = context.make_id(rel.schema.name, company.id, officer.id, role)
    rel.add("startDate", parse_date(data.pop("algus_kpv")))
    rel.add("endDate", parse_date(data.pop("lopp_kpv")))
    rel.add("role", role)
    if schema == "Ownership":
        rel.add("owner", officer)
        rel.add("asset", company)
    elif schema == "Directorship":
        rel.add("director", officer)
        rel.add("organization", company)
    return rel


def parse_general(context: Context, row: Item):
    data = row.pop("yldandmed")
    legal_form = data.pop("oiguslik_vorm_tekstina")
    proxy = make_proxy(context, row, TYPES.get(legal_form, "Company"))
    proxy.add("legalForm", legal_form)
    for item in data.pop("staatused"):
        if item["staatus"] == "R":
            proxy.add("incorporationDate", parse_date(item["algus_kpv"]))
    for item in get_value(data, ("aadressid", "kontaktisiku_aadressid"), []):
        proxy.add("address", get_address(item))
    proxy.add(
        "status", get_value(data, ("staatus_tekstina", "ettevotja_staatus_tekstina"))
    )

    for contact in data.pop("sidevahendid", []):
        value = contact["sisu"].strip(".")
        if contact["liik"] == "EMAIL":
            proxy.add("email", value)
        elif contact["liik"] == "WWW":
            proxy.add("website", value)
        elif contact["liik"] in ("MOB", "TEL"):
            proxy.add("phone", value)

    context.emit(proxy)


def parse_officer(context: Context, row: Item):
    company = make_proxy(context, row)
    assert company.id is not None, ("Company ID is None", row)
    context.emit(company)

    for data in get_value(row, ("kaardile_kantud_isikud", "kaardivalised_isikud")):
        officer = make_officer(context, data, company.id)
        rel_type = data.pop("isiku_roll")
        role = data.pop("isiku_roll_tekstina")
        if rel_type == "O":
            rel = make_rel(context, company, officer, "Ownership", data, role)
            rel.add("percentage", data.pop("osaluse_protsent"))
            rel.add("sharesValue", data.pop("osaluse_suurus"))
            rel.add("sharesCurrency", data.pop("osaluse_valuuta"))
        else:
            rel = make_rel(context, company, officer, "Directorship", data, role)
        context.emit(officer)
        context.emit(rel)


def parse_bfo(context: Context, row: Item):
    company = make_proxy(context, row)
    assert company.id is not None, ("Company ID is None", row)
    context.emit(company)

    for data in row.pop("kasusaajad"):
        officer = make_officer(context, data, company.id)
        rel = make_rel(
            context,
            company,
            officer,
            "Ownership",
            data,
            data.pop("kontrolli_teostamise_viis_tekstina"),
        )
        context.emit(officer)
        context.emit(rel)


def parse_json(context: Context, source: str) -> Generator[Item, None, None]:
    data_path = context.fetch_resource(source, PATTERN % source)
    idx = 0
    with zipfile.ZipFile(data_path) as zipfh:
        for name in zipfh.namelist():
            if name.endswith(".json"):
                with zipfh.open(name, "r") as fh:
                    items = ijson.items(fh, "item")
                    for idx, item in enumerate(items):
                        yield item
                        if idx and idx % 10_000 == 0:
                            context.log.info("Parse ijson item %d ..." % idx)
    context.log.info("Parsed %d ijson items." % (idx + 1), fp=data_path.name)


def crawl(context: Context) -> None:
    # general data
    for item in parse_json(context, "ettevotja_rekvisiidid__yldandmed.json.zip"):
        parse_general(context, item)

    # officers
    files = (
        "ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip",
        "ettevotja_rekvisiidid__kandevalised_isikud.json.zip",
    )
    for file in files:
        for item in parse_json(context, file):
            parse_officer(context, item)

    # bfo data
    for item in parse_json(context, "ettevotja_rekvisiidid__kasusaajad.json.zip"):
        parse_bfo(context, item)
