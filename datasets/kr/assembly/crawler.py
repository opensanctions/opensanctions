from typing import Any

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(row.pop("monaCd"))
    person.add("name", row.pop("hgNm"))  # data.lang=kor default
    person.add("name", row.pop("engNm"), lang="eng")
    person.add("alias", row.pop("hjNm"))
    person.add("gender", row.pop("sexGbnNm"))  # type.gender lookup
    person.add("political", row.pop("polyNm"))  # party affiliation
    # A few members pack two addresses into one field; split via the type.email lookup.
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
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    # The endpoint caps the page size silently; request all seats in one call and assert
    # the returned count matches the reported total.
    data = context.fetch_json(
        context.data_url,
        method="POST",
        data={"page": "1", "rows": 500},
        cache_days=1,
    )
    rows = data["data"]
    # Fail loudly if the single-request assumption breaks (paging silently re-introduced,
    # or the seat count grows past ROWS and the response is truncated).
    assert len(rows) == data["total"], (len(rows), data["total"])
    for row in rows:
        crawl_member(context, position, categorisation, row)
