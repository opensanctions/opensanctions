import json
from typing import Any

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(row.pop("monaCd"))
    person.add("name", row.pop("hgNm"))
    person.add("name", row.pop("engNm"), lang="eng")
    person.add("alias", row.pop("hjNm"))
    person.add("gender", row.pop("sexGbnNm"))
    person.add("political", row.pop("polyNm"))
    person.add("email", row.pop("eMail"))
    person.add("website", row.pop("homepage"))
    person.add("sourceUrl", row.pop("linkUrl"))
    h.apply_date(person, "birthDate", row.pop("bthDate"))
    # Public Official Election Act (공직선거법) Art. 16(2) requires Korean citizenship
    # to be elected: https://www.law.go.kr/법령/공직선거법/제16조
    person.add("citizenship", "kr")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is not None:
        occupancy.add("constituency", row.pop("origNm", None))
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(
        row,
        ignore=[
            "ROW_NUM",
            "age",
            "deptImgUrl",
            "polyCd",
            "eleGbnNm",
            "reeleGbnNm",
            "unitCd",
            "units",
            "unitNm",
            "cmitNm",
            "cmits",
            "telNo",
            "staff",
            "secretary",
            "secretary2",
            "openNaId",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of the Republic of Korea",
        wikidata_id="Q14850694",
        country="kr",
        topics=["gov.legislative", "gov.national"],
    )
    categorisation = categorise(context, position)
    context.emit(position)

    # The source has a flaky authoritative DNS setup that the crawl host's resolver
    # intermittently fails to resolve (and negatively caches the failure, so retries
    # don't help). Fetch via Zyte, which resolves the host on its own infrastructure.
    # The endpoint caps the page size silently; request all seats in one call and assert
    # the returned count matches the reported total.
    _, _, _, path = zyte_api.fetch_resource(
        context,
        "members.json",
        context.data_url,
        method="POST",
        body=b"page=1&rows=500",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    rows = data["data"]
    # Fail loudly if the single-request assumption breaks (paging silently re-introduced,
    # or the seat count grows past ROWS and the response is truncated).
    assert len(rows) == data["total"], (len(rows), data["total"])
    for row in rows:
        crawl_member(context, position, categorisation, row)
