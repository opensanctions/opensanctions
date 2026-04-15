import json
from rigour.mime.types import JSON

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise

IGNORE = ["deelstaatsenator", "fotowebpath", "kieskring", "link", "zetel"]


def crawl_member(
    context: Context,
    position: Entity,
    node: dict,
) -> None:
    member_id = node.pop("id")
    assert member_id is not None, "Missing id attribute on volksvertegenwoordiger"

    is_current = node.pop("is-huidige-vv")
    if is_current != "J":
        context.log.warning(
            "Skipping non-current member",
            id=member_id,
            is_huidige_vv=is_current,
        )
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
    if link is not None:
        detail_url = link[0]["href"]
        path = context.fetch_resource(
            f"vv_{member_id}.json",
            detail_url,
            headers={"Accept": "application/json;charset=UTF-8"},
        )
        with open(path, "r") as fh:
            data = json.load(fh)
        h.apply_date(person, "birthDate", data.pop("geboortedatum"))
        person.add("gender", data.pop("geslacht"))

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
    )
    if occupancy is not None:
        context.emit(person)
        context.emit(occupancy)

    context.audit_data(node, ignore=IGNORE)


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.json",
        context.data_url,
        headers={"Accept": "application/json;charset=UTF-8"},
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
    )
    # TODO: add "Flanders" as a subnational area once it's in 'countrynames'
    # https://github.com/opensanctions/countrynames/issues/55
    context.emit(position)

    for item in data["items"]:
        for member in item["kieskringlijst"]["volksvertegenwoordiger"]:
            crawl_member(context, position, member)
