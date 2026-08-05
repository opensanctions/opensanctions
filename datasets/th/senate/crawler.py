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

    person = context.make("Person")
    person.id = context.make_id(record.pop("MEMBER_CODE"), raw_name)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="tha", original_value=original_name)
    if clean_name is not None:
        apply_translit_full_name(context, person, LangText(clean_name, "tha"))
    # A candidate for the Senate must be of Thai nationality by birth (Constitution of
    # Thailand 2017, Section 108(1)).
    # https://www.constituteproject.org/constitution/Thailand_2017
    person.add("citizenship", "th")

    start_date = record.pop("START_DATE", None) or record.pop("MEMBER_STARTDATE", None)
    end_date = record.pop("END_DATE", None) or record.pop("MEMBER_ENDDATE", None)
    # Both sets state why a membership ended, in differently-named columns.
    end_reason = record.pop("RESIGN", None) or record.pop("MEMBER_END", None)
    # For a member who died in office, the membership ends on the date of death. Set it
    # before make_occupancy, which reads it to decide whether exposure still applies.
    if end_reason == "เสียชีวิต":  # deceased
        h.apply_date(person, "deathDate", end_date)

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
    # A stated reason for leaving without a usable date leaves the occupancy looking
    # current, so surface it rather than publishing them as a sitting senator.
    if end_reason is not None and not occupancy.has("endDate"):
        context.log.warning(
            "Membership ended but no end date could be parsed",
            name=raw_name,
            reason=end_reason,
            end_date=end_date,
        )
    # Current senators are selected by their peers within one of 20 occupational
    # groups, which stands in for a geographic constituency.
    group = record.pop("MEMBER_TYPE", None)
    if group:
        occupancy.add("constituency", group, lang="tha")

    context.audit_data(
        record,
        ignore=[
            "_id",
            # Current-set columns
            "POSITION",
            "COUNCIL_MEMBER",
            # Past-set columns
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


def fetch_records(
    context: Context, search_url: str, resource_id: str
) -> list[dict[str, Any]]:
    # datastore_search returns one page at a time; page through it by offset until we've
    # read every record the API reports, so we never silently drop the tail of a set.
    records: list[dict[str, Any]] = []
    total = None
    while total is None or len(records) < total:
        result = context.fetch_json(
            search_url,
            params={"resource_id": resource_id, "offset": len(records)},
            cache_days=14,
        )["result"]
        total = result["total"]
        page = result["records"]
        if len(page) == 0:
            raise RuntimeError(
                f"Datastore returned an empty page at offset {len(records)} "
                f"of {total} for resource {resource_id}"
            )
        records.extend(page)
    return records


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Thailand",
        country="th",
        topics=["gov.national", "gov.legislative"],
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
        records = fetch_records(context, search_url, resource["id"])
        context.log.info(
            "Crawling Senate set", name=resource["name"], members=len(records)
        )
        for record in records:
            crawl_member(context, position, categorisation, record)
