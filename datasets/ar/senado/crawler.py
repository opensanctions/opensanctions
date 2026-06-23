from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import OccupancyStatus, PositionCategorisation, categorise


def fetch_rows(context: Context, url: str) -> list[dict[str, Any]]:
    data = context.fetch_json(url)
    rows = data["table"]["rows"]
    if not isinstance(rows, list) or len(rows) == 0:
        raise ValueError(f"Expected a non-empty list of senators from {url}")
    return rows


def crawl_senator(
    context: Context,
    row: dict[str, Any],
    current: dict[str, Any] | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    senator_id = row.pop("ID")
    name = row.pop("SENADOR")
    start_date = row.pop("INICIO PERIODO REAL")
    end_date = row.pop("CESE PERIODO REAL")
    # The legal period is the senator's full mandate; the real dates are when they
    # actually took and left the seat (earlier/later for replacements).
    period_start = row.pop("INICIO PERIODO LEGAL")
    period_end = row.pop("CESE PERIODO LEGAL")
    province = row.pop("PROVINCIA")
    party = row.pop("PARTIDO POLITICO O ALIANZA")

    person = context.make("Person")
    person.id = context.make_slug(senator_id)
    if current is not None:
        # The current-members feed has properly-cased names and an email.
        h.apply_name(
            person,
            first_name=current["NOMBRE"],
            last_name=current["APELLIDO"],
        )
        person.add("email", current["EMAIL"])
    else:
        # The historical feed gives "APELLIDO, NOMBRE" in upper case.
        last_name, _, first_name = name.partition(",")
        h.apply_name(
            person,
            first_name=first_name.strip() or None,
            last_name=last_name.strip(),
        )
    # Senators must have been citizens of the Nation for six years (Constitution of
    # Argentina, Art. 55). https://www.constituteproject.org/constitution/Argentina_1994
    person.add("citizenship", "ar")
    person.add("political", party)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", province)
    # The bloque (parliamentary group) is distinct from party membership. The current
    # feed only reflects the present group, so apply it only to the senator's sitting
    # term; past terms have no bloque source and keep politicalGroup empty.
    if current is not None and OccupancyStatus.CURRENT.value in occupancy.get("status"):
        occupancy.add("politicalGroup", current["BLOQUE"])

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=["REEMPLAZO", "OBSERVACIONES"])


def crawl(context: Context) -> None:
    current = {row["ID"]: row for row in fetch_rows(context, context.data_url)}
    historico = fetch_rows(context, context.data_url.replace("/json", "Historico/json"))

    # The two feeds are joined on ID to enrich sitting senators; if that overlap ever
    # disappears, the join is broken and every senator silently loses email + bloque.
    historico_ids = {row["ID"] for row in historico}
    if not current.keys() & historico_ids:
        raise ValueError("No ID overlap between current and historical senator feeds")

    position = h.make_position(
        context,
        name="Member of the Argentine Chamber of Senators",
        country="ar",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18711738",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in historico:
        crawl_senator(context, row, current.get(row["ID"]), position, categorisation)
