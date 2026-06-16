from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


def parse_party_names(doc: Element) -> dict[str, str]:
    """Map each parliamentary-group GUID to its abbreviation from the filter checkboxes."""
    names: dict[str, str] = {}
    for checkbox in h.xpath_elements(doc, ".//input[@name='grupo-parlamentario']"):
        guid = checkbox.get("value")
        if guid is None:
            continue
        label = h.xpath_element(doc, ".//label[@for='%s']" % guid)
        names[guid] = h.element_text(label)
    return names


def crawl_deputy(
    context: Context,
    anchor: Element,
    party_names: dict[str, str],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    anchor_id = anchor.get("id")
    if anchor_id is None:
        raise ValueError("Deputy anchor without id")
    guid = anchor_id.removeprefix("diputado-")
    name = h.element_text(
        h.xpath_element(anchor, ".//p[contains(@class, 'diputado-index-nombre')]")
    )

    person = context.make("Person")
    person.id = context.make_slug(guid)
    person.add("name", name)
    person.add("gender", anchor.get("data-sexo"))  # via type.gender lookup (M/F)
    # Deputies must be Salvadoran by birth and the child of a Salvadoran parent
    # (Constitution of El Salvador, Art. 126); naturalised citizenship is not sufficient.
    # https://www.asamblea.gob.sv/sites/default/files/documents/decretos/171117_073157406_archivo_documento_legislativo.pdf
    person.add("citizenship", "sv")

    # Parliamentary group: store the abbreviation shown on the site, plus the full party
    # name when we have a mapping for it.
    group_guid = anchor.get("data-grupo-parlamentario")
    abbr = party_names.get(group_guid) if group_guid is not None else None
    if abbr is not None:
        person.add("political", abbr)
        result = context.lookup("party", abbr)
        if result is not None and result.name is not None:
            person.add("political", result.name)
        else:
            context.log.warning("Unmapped parliamentary group", abbr=abbr)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    party_names = parse_party_names(doc)

    # Each titular deputy is an anchor whose id is "diputado-<GUID>".
    anchors = h.xpath_elements(doc, ".//a[starts-with(@id, 'diputado-')]")
    if len(anchors) < 50:
        raise ValueError("Expected at least 50 deputies, found %d" % len(anchors))

    position = h.make_position(
        context,
        name="Member of the Legislative Assembly of El Salvador",
        country="sv",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328618",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    for anchor in anchors:
        crawl_deputy(context, anchor, party_names, position, categorisation)
