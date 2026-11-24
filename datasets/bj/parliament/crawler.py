from normality import normalize
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def name_key(name: str) -> str | None:
    # The groups page drops case and accents: "AKPONNA KINGNIDE PAULIN".
    return normalize(name, ascii=True)


def crawl_parties(context: Context) -> dict[str | None, str]:
    doc = context.fetch_html(
        "https://assemblee-nationale.bj/index.php/presentation/organisation_et_fonctionnement/groupes-parlementaires/"
    )
    parties: dict[str | None, str] = {}
    for table in h.xpath_elements(doc, "//table[contains(@class, 'tablepress')]"):
        for row in h.parse_html_table(table):
            cells = h.cells_to_str(row)
            name = cells.pop("nom_et_prenoms")
            party = cells.pop("parti")
            assert name is not None and party is not None, (name, party)
            parties[name_key(name)] = party
            context.audit_data(cells)
    return parties


def crawl_deputy(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    parties: dict[str | None, str],
    name: str,
) -> None:
    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add("citizenship", "bj")
    party = parties.get(name_key(name))
    if party is None:
        context.log.info("Deputy not on the parliamentary groups page", name=name)
    person.add("political", party)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Benin",
        wikidata_id="Q21290849",
        country="bj",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    parties = crawl_parties(context)
    doc = context.fetch_html(context.data_url)
    # No legislature label: pin the table so a new one fails the run.
    table = h.xpath_element(doc, "//table[@id='tablepress-75']")
    for row in h.parse_html_table(table):
        cells = h.cells_to_str(row)
        name = cells.pop("nom_prenoms")
        assert name is not None, cells
        crawl_deputy(context, position, categorisation, parties, name)
        context.audit_data(cells)
