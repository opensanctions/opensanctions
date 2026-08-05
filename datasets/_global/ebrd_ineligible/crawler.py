from typing import Any

from normality import collapse_spaces

from zavod import Context
from zavod import helpers as h


def crawl_entity(context: Context, data: dict[str, Any]) -> None:
    name_raw = data.pop("title")
    if not name_raw:
        return
    address = data.pop("address")
    country = collapse_spaces(data.pop("nationality"))
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name_raw, address, country)
    subtitle = data.pop("subtitle", "")
    original = h.Names(name=name_raw, alias=subtitle)
    h.apply_reviewed_names(context, entity, original=original, llm_cleaning=True)
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
    # They mis-classify some persons as companies
    context.audit_data(data, ignore=["projectNoticeType"])


def crawl(context: Context) -> None:
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
