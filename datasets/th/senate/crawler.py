from typing import Any
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import LangText


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    raw_name = record.pop("MEMBER_NAME")
    clean_name = h.strip_name_titles(context, raw_name)
    assert clean_name

    person = context.make("Person")
    person.id = context.make_id(record.pop("MEMBER_CODE"), raw_name)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="tha", original_value=original_name)
    apply_translit_full_name(context, person, LangText(clean_name, "tha"))
    # A candidate for the Senate must be of Thai nationality by birth (Constitution of
    # Thailand 2017, Section 108(1)).
    # https://www.constituteproject.org/constitution/Thailand_2017
    person.add("citizenship", "th")

    start_date = record.pop("START_DATE", record.pop("MEMBER_STARTDATE"))
    end_date = record.pop("END_DATE", record.pop("MEMBER_ENDDATE"))
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # current senators represent one of 20 occupational working groups
    group = record.pop("MEMBER_TYPE", None)
    if group:
        occupancy.add("description", group, lang="tha")

    context.audit_data(
        record,
        ignore=[
            "_id",
            # Current-set columns
            "POSITION",
            "RESIGN",
            "COUNCIL_MEMBER",
            # Past-set columns
            "MEMBER_END",
            "COUNCIL_NAME",
            "COUNCIL_START",
            "COUNCIL_END",
            "COUNCIL",
            # Common
            "COUNCIL_YEAR",
            "COUNCIL_NO",
        ],
    )
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Thailand",
        country="th",
        wikidata_id="Q21295152",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Crawl every Senate set it lists, current and past, from the datastore API. Each set is
    # published as a CSV and an XLSX backed by the same datastore; read the CSV so we
    # don't crawl the same records twice.
    package = context.fetch_json(context.data_url, cache_days=1)
    search_url = urljoin(context.data_url, "/api/3/action/datastore_search")

    for resource in package["result"]["resources"]:
        if (resource.get("format") or "").upper() != "CSV":
            continue
        # A Senate set has a few hundred members; one large page fetches them all.
        result = context.fetch_json(
            search_url,
            params={"resource_id": resource["id"], "limit": 10000},
            cache_days=14,
        )["result"]
        records = result["records"]
        context.log.info(
            "Crawling Senate set", name=resource["name"], members=len(records)
        )
        for record in records:
            crawl_member(context, position, categorisation, record)
