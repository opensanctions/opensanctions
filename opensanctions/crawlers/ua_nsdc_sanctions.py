import os
from itertools import count
from typing import Dict, Any

from opensanctions.core import Context
from opensanctions import helpers as h

PASSWORD = os.environ.get("OPENSANCTIONS_NSDC_PASSWORD")
AUTH = ("rnbo", PASSWORD)

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


def crawl(context: Context) -> None:
    for page in count(1):
        params = {"perPage": 100, "page": page}
        response = context.fetch_json(
            context.data_url,
            auth=AUTH,
            params=params,
            # cache_days=1,
        )
        for item in response["data"]:
            crawl_item(context, item)
        if page == response["pages"]:
            break
