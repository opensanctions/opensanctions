from typing import Any, Dict
from normality import collapse_spaces
import re

from zavod import Context
from zavod import helpers as h

NAME_SPLITS = [
    "may also be doing business as",
    "also doing business as",
    "doing business as",
    "also doing business under",
    "also known as",
    " or ",
    "f/k/a",
    "formerly known as",
    "formerly operating as",
    "formerly",
]
RE_NAME_SPLIT = re.compile("|".join(NAME_SPLITS), re.IGNORECASE)


def get_schema(context: Context, name: str, projectNoticeType: str) -> str:
    schema = context.lookup_value("override_schema", name)
    if schema is not None:
        return schema
    schema = context.lookup_value("schema", projectNoticeType)
    if schema is None:
        context.log.warning(f"Unknown schema: {projectNoticeType}")
        schema = "LegalEntity"
    return schema


def crawl_entity(context: Context, data: Dict[str, Any]):
    name_raw = data.pop("title")
    address = data.pop("address")
    country = collapse_spaces(data.pop("nationality"))
    schema = get_schema(context, name_raw, data.pop("projectNoticeType"))
    entity = context.make(schema)
    entity.id = context.make_id(name_raw, address, country)
    entity.add("name", RE_NAME_SPLIT.split(name_raw))
    entity.add("address", address.split("$"))
    entity.add("country", country)

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", data.pop("prohibitedPractice"))
    sanction.set("authority", data.pop("originatingInstitution"))
    sanction.add("country", data.pop("jurisdictionOfJudgement"))
    h.apply_date(sanction, "startDate", data.pop("ineligibleFromDate"))
    h.apply_date(sanction, "endDate", data.pop("ineligibleUntilDate"))
    h.apply_date(sanction, "date", data.pop("dateNoticeEffectiveAtEBRD"))
    if h.is_active(sanction):
        entity.add("topics", "debarment")
    context.emit(entity)
    context.emit(sanction)
    context.audit_data(data)


def crawl(context: Context):
    currentPage = 1
    data = {
        "parentPath": "/content/dam/ebrd_dxp/content-fragments/occo/ineligible-entities",
        "cardType": "iecard",
        "sortBy": "newest-first",
    }
    results = None
    while results is None or len(results) > 0:
        data["currentPage"] = str(currentPage)
        doc = context.fetch_json(context.data_url, method="POST", data=data)
        results = doc.get("searchResult")
        for result in results:
            crawl_entity(context, result)
        currentPage += 1
