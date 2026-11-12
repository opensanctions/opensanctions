import re
from typing import Any

import requests

from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import Context, Entity
from zavod import helpers as h
from zavod.util import Element


def fetch_filing(context: Context, barcode: int) -> tuple[str, Element | None]:
    """Fetch a filing of either system as an HTML report.

    Returns the report URL, and the document or None if the server fails."""
    url = (
        "https://obsidian.antikorupcija.me/api/registri/reports/report"
        "?reportname=registar_izvjestaja_o_prihodima_i_imovini_funkcionera_izvjestaj"
        f"&reporttype=html&parm_01={int(barcode)}"
    )
    try:
        # Filings don't change. The response omits its UTF-8 charset.
        return url, context.fetch_html(url, cache_days=90, encoding="utf-8")
    except (requests.ConnectionError, requests.Timeout) as exc:
        # The server sometimes stalls for up to an hour, then times out.
        context.log.warning("Failed to fetch filing", url=url, error=str(exc))
        return url, None


def read_section(doc: Element, title: str) -> list[dict[str, str | None]]:
    """Read the non-empty rows of the report table titled `title`."""
    table = h.xpath_element(
        doc, f"//table[tr[1]/th[starts-with(normalize-space(.), '{title}')]]"
    )
    rows = [h.cells_to_str(row) for row in h.parse_html_table(table, skiprows=1)]
    return [row for row in rows if any(value is not None for value in row.values())]


def build_person_from_family_row(
    context: Context, row: dict[str, str | None], index_name: str | None = None
) -> Entity:
    first_name = row.pop("ime")
    last_name = row.pop("prezime")
    # Prefer the index's spelling when only the case differs (AMIR ĐOKAJ).
    full_name = h.make_name(first_name=first_name, last_name=last_name)
    if (
        index_name is not None
        and full_name is not None
        and index_name.casefold() == full_name.casefold()
    ):
        full_name = index_name
    # Empty maiden names are written as '-/-'; some hold a birth date instead.
    maiden_name = (row.pop("rodeno_prezime") or "").strip("-").strip("/") or None
    dob = None
    if maiden_name is not None and re.fullmatch(r"[.\d]+", maiden_name):
        dob, maiden_name = maiden_name, None

    city = row.pop("mjesto")
    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, city)
    entity.add("citizenship", (row.pop("drzavljanstvo") or "").split(","))
    h.apply_name(
        entity,
        full=full_name,
        first_name=first_name,
        last_name=last_name,
        maiden_name=maiden_name,
    )
    h.apply_date(entity, "birthDate", dob)
    residence = row.pop("boraviste")
    if residence is not None and city is not None:
        if residence.casefold() == city.casefold():
            residence = None
    address = h.make_address(context, city=city, place=residence, lang="cnr")
    h.copy_address(entity, address)
    context.audit_data(row, ["srodstvo"])  # used by the caller
    return entity


def crawl_relatives(
    context: Context, person: Entity, relatives: list[dict[str, str | None]]
) -> None:
    for row in relatives:
        relationship = row.pop("srodstvo")
        relative = build_person_from_family_row(context, row)
        # Children are often listed by their initials only.
        if len(relative.caption) < 4:
            continue
        relative.add("topics", "role.rca")
        context.emit(relative)

        rel = context.make("Family")
        rel.id = context.make_id(person.id, relationship, relative.id)
        rel.add("relationship", relationship)
        rel.add("person", person.id)
        rel.add("relative", relative.id)
        context.emit(rel)


def parse_function(
    context: Context, person: Entity, row: dict[str, str | None]
) -> tuple[Entity, str | None, str | None, OccupancyStatus | None] | None:
    """Make the position and dates for a function listed in a filing."""
    function = row.pop("naziv_funkcije")
    organization = row.pop("organ_vlasti_gdje_se_obavlja_funkcija")
    if function is None or organization is None:
        context.log.warning(
            "Function without a name or organisation",
            person=person.id,
            function=function,
            organization=organization,
        )
        return None
    ended = row.pop("prestala_funkcija")
    if ended not in (None, "X"):
        context.log.warning("Unexpected 'ended' marker", value=ended)
    start_date = row.pop("datum_pocetka")
    end_date = row.pop("datum_prestanka")
    context.audit_data(
        row,
        ignore=[
            "organ_vlasti_koji_bira_imenuje_postavlja",
            "mjesto_obavljanja",
            "bez_naknade",
        ],
    )

    position_name = f"{function}, {organization}"
    person.add("position", position_name)
    position = h.make_position(
        context,
        position_name,
        country="me",
        lang="cnr",
        translate_name=True,
    )
    # Some functions are marked as ended without an end date.
    status = OccupancyStatus.ENDED if ended == "X" and end_date is None else None
    return position, start_date, end_date, status


def crawl_person(context: Context, person_data: dict[str, Any]) -> None:
    full_name = person_data.pop("imeIPrezime")
    filings: list[dict[str, Any]] = person_data.pop("izvjestajImovine")
    function_labels: list[str] = person_data.pop("nazivFunkcije")

    filing_date: str | None = None
    source_url: str | None = None
    family_rows: list[dict[str, str | None]] = []
    function_rows: list[dict[str, str | None]] = []
    if filings:
        # A person can file more than once on the same day.
        latest = max(filings, key=lambda f: (f["datum"], f["barcode"]))
        filing_date = latest["datum"]
        source_url, doc = fetch_filing(context, latest["barcode"])
        # Without the report, fall back to the index labels below.
        if doc is not None:
            family_rows = read_section(doc, "2.2 Članovi porodice")
            function_rows = read_section(doc, "2.4 Javna funkcija")

    # The official is listed among their family as "Funkcioner".
    officials = [row for row in family_rows if row.get("srodstvo") == "Funkcioner"]
    relatives = [row for row in family_rows if row.get("srodstvo") != "Funkcioner"]
    if len(officials) > 1:
        context.log.warning("Multiple officials in a filing", name=full_name)

    if officials:
        person = build_person_from_family_row(context, officials[0], full_name)
        # Filings may use Cyrillic, drop diacritics or give a married name.
        h.apply_name(person, full_name)
    else:
        person = context.make("Person")
        # TODO(Leon Handreke): I think we should be passing *function_labels here,
        # but that would mean a re-key.
        person.id = context.make_id(full_name, sorted(function_labels))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
        h.apply_name(person, full_name)
    # Appointed officials need not be citizens. Filings state citizenship.
    person.add("country", "me")

    held: list[tuple[Entity, str | None, str | None, OccupancyStatus | None]] = []
    if function_rows:
        for row in function_rows:
            function = parse_function(context, person, row)
            if function is not None:
                held.append(function)
    else:
        # People without a filing still owe one for the function, so keep them.
        status = OccupancyStatus.UNKNOWN if filing_date is None else None
        for label in function_labels:
            position = h.make_position(context, label, country="me")
            held.append((position, None, None, status))

    is_national = False
    for position, start_date, end_date, status in held:
        categorisation = categorise(context, position, default_is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            categorisation=categorisation,
            start_date=start_date,
            end_date=end_date,
            declaration_date=filing_date,
            no_end_implies_current=False,  # a filing is a snapshot
            status=status,
        )
        if occupancy is None:
            continue
        occupancy.add("sourceUrl", source_url)
        if "gov.judicial" in categorisation.topics:
            person.add("topics", "role.judge")

        context.emit(position)
        context.emit(occupancy)
        context.emit(person)
        if "gov.national" in categorisation.topics:
            is_national = True

    if is_national:
        crawl_relatives(context, person, relatives)


def crawl(context: Context) -> None:
    page = 0
    # About 9 pages in 2026.
    max_pages = 20
    while True:
        # 2000 is the most the API serves per page.
        people = context.fetch_json(f"{context.data_url}?page={page}&size=2000")
        if not people:
            break
        for person in people:
            crawl_person(context, person)
        page += 1
        if page >= max_pages:
            context.log.warning(
                f"Emergency exit: Reached the maximum page limit of {max_pages}."
            )
            break
