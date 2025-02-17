import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin
from rigour.ids import INN
from followthemoney.types import registry

from zavod import Context, Entity
from zavod import helpers as h

PASSWORD = os.environ.get("OPENSANCTIONS_NSDC_PASSWORD")
API_KEY = os.environ.get("OPENSANCTIONS_NSDC_API_KEY")
CACHE_LONG = 7
# They seem to mix up ukr, ukr, rus and ukr, rus, ukr so not assuming
REGEX_NAME_3_PARTS = re.compile(r"^([^\(]+)\(([^,]+),([^,]+)\)$")
REGEX_ADDR_2_PARTS = re.compile(r"^([^\(]{75,})\((.{75,})\)$")


def fetch_data(
    context: Context, path: str, cache_days: Optional[int] = None
) -> List[Dict[str, Any]]:
    assert API_KEY, "Missing $OPENSANCTIONS_NSDC_API_KEY!"
    headers = {"x-cota-public-api-key": API_KEY}
    url = urljoin(context.data_url, path)
    return context.fetch_json(url, headers=headers, cache_days=cache_days)


def clean_address(value: str) -> List[str]:
    if match := REGEX_ADDR_2_PARTS.match(value):
        return match.groups()
    return value


def note_long_identifier(entity: Entity, values: List[str]) -> None:
    for value in values:
        if len(value) > registry.identifier.max_length:
            entity.add("notes", value, lang="ukr")


def check_sanctioned(
    context: Context, entity: Entity, subject_id: int, item: Dict[str, Any], status: str
) -> bool | None:
    res = context.lookup("sanctioned_status", status)
    if res is None:
        context.log.warn(
            "Unknown status",
            status=status,
            subject_id=subject_id,
            entity_name=entity.get("name"),
        )
        return True
    else:
        return res.value.lower() == "true"


def crawl_common(
    context: Context, subject_id: str, entity: Entity, item: Dict[str, Any]
) -> None:
    status = item.pop("status")
    if status is None:
        return
    name = item.pop("name")
    if match := REGEX_NAME_3_PARTS.match(name):
        entity.add("name", match.groups())
    else:
        entity.add("name", name, lang="ukr")

    name_translit = item.pop("translit")
    if match := REGEX_NAME_3_PARTS.match(name_translit):
        entity.add("name", match.groups())
    else:
        entity.add("name", name_translit, lang="eng")

    identifiers = item.pop("identifiers") or []
    for ident in identifiers:
        ident_id = ident.pop("id")
        ident_values = h.multi_split(ident.pop("code"), ";")
        res = context.lookup("identifier_type", ident_id)
        if res is None:
            context.log.warn(
                "Unknown identifier type", id=ident_id, values=ident_values
            )
        elif res.prop:
            note_long_identifier(entity, ident_values)
            if res.prop == "innCode":
                if INN.is_valid(ident_values):
                    entity.add(res.prop, ident_values)
                else:
                    entity.add("taxNumber", ident_values)
            else:
                entity.add(res.prop, ident_values)
        elif res.identification:
            for value in ident_values:
                doc = h.make_identification(
                    context,
                    entity,
                    doc_type=ident_id,
                    number=value,
                    country=ident.get("iso2"),
                    summary=ident.get("note"),
                    passport=res.identification["is_passport"],
                )
                if doc is not None:
                    context.emit(doc)
        else:
            context.log.warn("Invalid identifier lookup", id=ident_id, res=res)

    attributes = item.pop("attributes") or []
    for attr in attributes:
        key = attr.pop("key")
        result = context.lookup("attributes", key)
        value = attr.pop("value")
        value = value.replace("(росія)", "")

        if result is not None:
            if result.prop is not None:
                if result.prop == "address":
                    value = clean_address(value)
                entity.add(result.prop, value, lang="ukr")
        elif key in ("КПП",):
            if entity.schema.is_a("Organization"):
                entity.add_cast("Company", "kppCode", value)
            else:
                entity.add("registrationNumber", value)
        else:
            override_res = context.lookup("overrides", f"{key}: {value}")
            if override_res:
                for override in override_res.items:
                    entity.add(override["prop"], override["value"], lang="ukr")
            else:
                context.log.info("Unknown attribute", key=key, value=value)
                entity.add("notes", f"{key}: {value}", lang="ukr")

    is_sanctioned = check_sanctioned(context, entity, subject_id, item, status)
    if is_sanctioned is None:
        return
    if is_sanctioned:
        entity.add("topics", "sanction")

    for action in fetch_data(
        context, f"/subjects/{subject_id}/actions", cache_days=CACHE_LONG
    ):
        action_id = action.pop("aid")
        sanction = h.make_sanction(context, entity, key=action_id)
        sanction.add("status", action.pop("status"))
        decree = action.pop("decree", None) or {}
        sanction.add("sourceUrl", decree.pop("link", None))
        sanction.add("startDate", decree.pop("date", None))
        sanction.add("program", decree.pop("number", None))
        sanction.add("endDate", action.pop("endDate", None))
        sanction.add("duration", action.pop("term", None), lang="ukr")
        sanction.add("authorityId", action_id)
        restrictions = action.pop("restrictions", None) or []
        for restr in restrictions:
            sanction.add("provisions", restr["restriction"])

        # Neither reason nor reasons has value nowadays, keeping it just in case
        sanction.add("reason", action.pop("reasons", None), lang="ukr")
        sanction.add("reason", action.pop("reason", None), lang="ukr")
        sanction.add("authority", action.pop("issuers", None), lang="ukr")
        context.emit(sanction)

    context.audit_data(item)
    context.emit(entity, target=is_sanctioned)


def crawl_individual(context: Context, item: Dict[str, Any]) -> None:
    subject_id = item.pop("sid")
    entity = context.make("Person")
    entity.id = context.make_slug(subject_id, item.get("name"))
    entity.add("alias", item.pop("aliases"))
    entity.add("citizenship", item.pop("citizenships"))
    entity.add("birthDate", item.pop("bd"))
    entity.add("deathDate", item.pop("dd"))
    crawl_common(context, subject_id, entity, item)


def crawl_legal(context: Context, item: Dict[str, Any]) -> None:
    subject_id = item.pop("sid")
    entity = context.make("Organization")
    entity.id = context.make_slug(subject_id, item.get("name"))
    entity.add("alias", item.pop("aliases"))
    entity.add("jurisdiction", item.pop("citizenships"))
    entity.add("incorporationDate", item.pop("bd"))
    entity.add("dissolutionDate", item.pop("dd"))
    crawl_common(context, subject_id, entity, item)


def crawl(context: Context) -> None:
    for item in fetch_data(context, "/v2/subjects?subjectType=individual"):
        crawl_individual(context, item)
    for item in fetch_data(context, "/v2/subjects?subjectType=legal"):
        crawl_legal(context, item)
