import json
from rigour.mime.types import JSON
from typing import Any, Dict, NamedTuple

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise

IGNORE = ["deelstaatsenator", "fotowebpath", "kieskring", "zetel"]
HEADERS = {"Accept": "application/json;charset=UTF-8"}


class MemberDetail(NamedTuple):
    gender: str
    birth_date: str
    start_date: str | None


def get_member_detail(
    context: Context, link: list[Dict[str, Any]], member_id: str
) -> MemberDetail:
    detail_url = link[0]["href"]
    path = context.fetch_resource(
        f"vv_{member_id}.json",
        detail_url,
        headers=HEADERS,
    )
    with open(path, "r") as fh:
        data = json.load(fh)
    # find the active mandate (no end date) to get occupancy start
    start_date = None
    for mandate in data.get("mandaat-vlaams-parlement", []):
        if not mandate.get("datumtot"):
            start_date = mandate.get("datumvan")
            break
    return MemberDetail(
        gender=data.pop("geslacht"),
        birth_date=data.pop("geboortedatum"),
        start_date=start_date,
    )


def crawl_member(context: Context, position: Entity, node: Dict[str, Any]) -> None:
    member_id = node.pop("id")
    assert member_id is not None

    is_current = node.pop("is-huidige-vv")
    if is_current != "J":
        context.log.warning("Skipping non-current member", id=member_id)
        return

    person = context.make("Person")
    person.id = context.make_slug("vv", member_id)
    h.apply_name(
        person,
        first_name=node.pop("voornaam"),
        last_name=node.pop("naam"),
    )
    person.add("citizenship", "be")
    link = node.pop("link")
    member_detail = None
    if link is not None:
        member_detail = get_member_detail(context, link, member_id)
        h.apply_date(person, "birthDate", member_detail.birth_date)
        person.add("gender", member_detail.gender)

    fractie_node = node.pop("fractie")
    if fractie_node is not None:
        person.add("political", fractie_node.pop("naam"))

    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        categorisation=categorisation,
        start_date=member_detail.start_date if member_detail else None,
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(occupancy)

    context.audit_data(node, ignore=IGNORE)


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.json",
        context.data_url,
        headers=HEADERS,
    )
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)

    position = h.make_position(
        context,
        name="Member of the Flemish Parliament",
        wikidata_id="Q15105064",
        topics=["gov.state", "gov.legislative"],
        lang="eng",
        subnational_area="be-vlg",
    )
    context.emit(position)

    for item in data["items"]:
        for member in item["kieskringlijst"]["volksvertegenwoordiger"]:
            crawl_member(context, position, member)
