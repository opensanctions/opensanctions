import os
from urllib.parse import urljoin
from typing import Dict, Any, List, Optional

from zavod import Context, Entity
from zavod import helpers as h

PASSWORD = os.environ.get("OPENSANCTIONS_NSDC_PASSWORD")
API_KEY = os.environ.get("OPENSANCTIONS_NSDC_API_KEY")
CODES = {"DN": "UA-DPR", "LN": "UA-LPR"}
CACHE_LONG = 7


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
        elif ident_id in ("reg:person_ro", "reg:person_il"):
            entity.add("idNumber", ident_value)
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
            context.log.warn("Unknown attribute", key=key, value=value)
            entity.add("notes", f"{key}: {value}", lang="ukr")

    for action in fetch_data(
        context, f"/subjects/{subject_id}/actions", cache_days=CACHE_LONG
    ):
        sanction = h.make_sanction(context, entity, key=action.pop("aid"))
        sanction.add("status", action.pop("status"))
        decree = action.pop("decree", None) or {}
        sanction.add("sourceUrl", decree.pop("link", None))
        sanction.add("startDate", decree.pop("date", None))
        sanction.add("program", decree.pop("number", None))
        sanction.add("endDate", action.pop("endDate", None))
        sanction.add("duration", action.pop("term", None), lang="ukr")
        restrictions = action.pop("restrictions", None) or []
        for restr in restrictions:
            sanction.add("provisions", restr["restriction"])

        # Neither reason nor reasons has value nowadays, keeping it just in case
        sanction.add("reason", action.pop("reasons", None), lang="ukr")
        sanction.add("reason", action.pop("reason", None), lang="ukr")
        sanction.add("authority", action.pop("issuers", None), lang="ukr")
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
    for item in fetch_data(context, "/v2/subjects?subjectType=individual"):
        crawl_indiviudal(context, item)

    for item in fetch_data(context, "/v2/subjects?subjectType=legal"):
        crawl_legal(context, item)
