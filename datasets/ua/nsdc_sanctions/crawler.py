import os
from itertools import count
from urllib.parse import urljoin
from typing import Dict, Any, List, Optional

from zavod import Context, Entity
from zavod import helpers as h

PASSWORD = os.environ.get("OPENSANCTIONS_NSDC_PASSWORD")
API_KEY = os.environ.get("OPENSANCTIONS_NSDC_API_KEY")
CODES = {"DN": "UA-DPR", "LN": "UA-LPR"}


def crawl_item(context: Context, item: Dict[str, Any]) -> None:
    subject_type = item.pop("subjectType")
    schema = context.lookup_value("subject_types", subject_type)
    if schema is None:
        context.log.warn("Unknown subject type", subject_type=subject_type)
        return

    entity = context.make(schema)
    subject_id = item.pop("subjectId")
    subject_name = item.pop("subjectName")
    entity.id = context.make_slug(subject_id, subject_name)
    entity.add("topics", "sanction")
    entity.add("name", subject_name)
    entity.add("alias", item.pop("subjectAliases"))

    dob = item.pop("subjectBirthDate", None)
    if entity.schema.is_a("Person"):
        entity.add("birthDate", dob)
    else:
        entity.add("incorporationDate", dob)

    for citz in item.pop("subjectCitizenships", None) or []:
        code = citz.pop("code", None)
        if code is None:
            context.log.info("Invalid citizenship", data=citz)
            continue

        code = CODES.get(code, code)
        if entity.schema.is_a("Person"):
            entity.add("nationality", code)
        else:
            entity.add("jurisdiction", code)

    for ident in item.pop("subjectIdentifiers", None) or []:
        ident_type = ident.pop("type", None)
        ident_code = ident.pop("code", None)
        result = context.lookup("ident_types", ident_type)
        if result is None:
            context.log.warning(
                "Unknown identifier type",
                type=ident_type,
                code=ident_code,
                country=ident.get("country"),
            )
            continue
        if result.schema:
            entity.add_schema(result.schema)
        if result.value is not None:
            entity.add(result.value, ident_code)

    sanction = h.make_sanction(context, entity)
    sanction.add("listingDate", item.pop("decreeDate"))
    sanction.add("startDate", item.pop("decreeActDate"))
    sanction.add("endDate", item.pop("actionEndDate"))
    sanction.add("sourceUrl", item.pop("decreeActLink"))
    sanction.add("sourceUrl", item.pop("decreeLink"))
    sanction.add("authorityId", item.pop("actionId"))
    sanction.add("program", item.pop("decreeNumber"))
    item.pop("actionType")
    item.pop("actionTypeName")
    sanction.add("status", item.pop("actionStatus"))
    sanction.add("duration", item.pop("actionTerm"))

    decree = item.pop("targetDecree", None) or {}
    sanction.add("sourceUrl", decree.pop("link", None))
    sanction.add("program", decree.pop("number", None))

    item.pop("internationalSanctions", None)
    context.audit_data(item)

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_old_system(context: Context) -> None:
    if PASSWORD is None:
        context.log.warn("Missing $OPENSANCTIONS_NSDC_PASSWORD!")
        return
    for page in count(1):
        params = {"perPage": 100, "page": page}
        response = context.fetch_json(
            "https://sanctions.nsdc.gov.ua/api/sanctions/",
            auth=("rnbo_user", PASSWORD),
            params=params,
            # cache_days=1,
        )
        for item in response["data"]:
            crawl_item(context, item)
        if page == response["pages"]:
            break


def fetch_data(
    context: Context, path: str, cache_days: Optional[int] = None
) -> List[Dict[str, Any]]:
    assert API_KEY, "Missing $OPENSANCTIONS_NSDC_API_KEY!"
    headers = {"x-cota-public-api-key": API_KEY}
    url = urljoin(context.data_url, path)
    return context.fetch_json(url, headers=headers, cache_days=cache_days)


def crawl_common(
    context: Context, subject_id: str, entity: Entity, item: Dict[str, Any]
) -> None:
    identifiers = item.pop("identifiers") or []
    for ident in identifiers:
        ident_id = ident.pop("id")
        ident_value = ident.pop("code")
        if ident_id == "tax:inn":
            entity.add("innCode", ident_value)
        elif ident_id in ("reg:odrn", "reg:odrnip"):
            entity.add("ogrnCode", ident_value)
        elif ident_id == "reg:okpo":
            entity.add("okpoCode", ident_value)
        elif ident_id in ("reg:edrpou", "reg:r_n", "reg:regon", "reg:unzr", None):
            entity.add("registrationNumber", ident_value)
        elif ident_id in (
            "doc:passport",
            "doc:d_passport",
            "doc:f_passport",
            "doc:s_passport",
        ):
            doc = h.make_identification(
                context,
                entity,
                doc_type=ident_id,
                number=ident_value,
                country=ident.get("iso2"),
                summary=ident.get("note"),
                passport=True,
            )
            if doc is not None:
                context.emit(doc)
        else:
            context.log.warn("Unknown identifier type", id=ident_id, value=ident_value)

    attributes = item.pop("attributes") or []
    for attr in attributes:
        key = attr.pop("key")
        result = context.lookup("attributes", key)
        value = attr.pop("value")
        value = value.replace("(росія)", "")
        if result is not None and result.prop is not None:
            entity.add(result.prop, value, lang="ukr")
        elif key in ("КПП",):
            if entity.schema.is_a("Organization"):
                entity.add_cast("Company", "kppCode", value)
            else:
                entity.add("registrationNumber", value)
        else:
            # context.log.warn("Unknown attribute", key=key, value=value)
            entity.add('notes', f"{key}: {value}", lang="ukr")

    for action in fetch_data(context, f"/subjects/{subject_id}/actions", cache_days=30):
        sanction = h.make_sanction(context, entity, key=action['aid'])
        sanction.add("status", action.pop("status"))
        decree = action.pop("decree", None) or {}
        sanction.add("sourceUrl", decree.pop("link", None))
        sanction.add("startDate", decree.pop("date", None))
        sanction.add("program", decree.pop("number", None))
        sanction.add("endDate", action.pop("endDate", None))
        sanction.add("duration", action.pop("term", None), lang="ukr")
        restrictions = action.pop("restrictions", None) or []
        for restr in restrictions:
            sanction.add('provisions', restr['restriction'])
        sanction.add('reason', action.pop('reason', None), lang="ukr")
        context.emit(sanction)

    entity.add("topics", "sanction")
    context.audit_data(item, ignore=["status"])
    context.emit(entity, target=True)


def crawl_indiviudal(context: Context, item: Dict[str, Any]) -> None:
    subject_id = item.pop("sid")
    name = item.pop("name")
    entity = context.make("Person")
    entity.id = context.make_slug(subject_id, name)
    entity.add("name", name, lang="ukr")
    entity.add("name", item.pop("translit"), lang="eng")
    entity.add("alias", item.pop("aliases"))
    entity.add("nationality", item.pop("citizenships"))
    entity.add("birthDate", item.pop("bd"))
    entity.add("deathDate", item.pop("dd"))
    crawl_common(context, subject_id, entity, item)


def crawl_legal(context: Context, item: Dict[str, Any]) -> None:
    subject_id = item.pop("sid")
    name = item.pop("name")
    entity = context.make("Organization")
    entity.id = context.make_slug(subject_id, name)
    entity.add("name", name, lang="ukr")
    entity.add("name", item.pop("translit"), lang="eng")
    entity.add("alias", item.pop("aliases"))
    entity.add("jurisdiction", item.pop("citizenships"))
    entity.add("incorporationDate", item.pop("bd"))
    entity.add("dissolutionDate", item.pop("dd"))
    crawl_common(context, subject_id, entity, item)


def crawl(context: Context) -> None:
    # crawl_old_system(context)
    for item in fetch_data(context, "/subjects/individual"):
        crawl_indiviudal(context, item)

    for item in fetch_data(context, "/subjects/legal"):
        crawl_legal(context, item)
