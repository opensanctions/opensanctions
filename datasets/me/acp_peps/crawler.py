import re
from typing import Any

import requests

from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import Context, Entity
from zavod import helpers as h
from zavod.util import Element


def fetch_filing(context: Context, barcode: int) -> Element | None:
    """Fetch a filing as the HTML report the register links to.

    The report covers filings from both the current system and the one before it.
    Returns None if the server doesn't deliver it."""
    url = (
        "https://obsidian.antikorupcija.me/api/registri/reports/report"
        "?reportname=registar_izvjestaja_o_prihodima_i_imovini_funkcionera_izvjestaj"
        f"&reporttype=html&parm_01={barcode}"
    )
    try:
        # A filed report doesn't change, so only new filings need fetching.
        # The response header omits the charset, which the page declares as UTF-8.
        return context.fetch_html(url, cache_days=90, encoding="utf-8")
    except (requests.ConnectionError, requests.Timeout) as exc:
        # The server sometimes stalls on a report for up to an hour and then
        # times out. Don't fail the whole run over one report.
        context.log.warning("Failed to fetch filing", url=url, error=str(exc))
        return None


def read_section(doc: Element, title: str) -> list[dict[str, str | None]]:
    """Read the rows of the report table whose title row starts with `title`."""
    table = h.xpath_element(
        doc, f"//table[tr[1]/th[starts-with(normalize-space(.), '{title}')]]"
    )
    rows = [h.cells_to_str(row) for row in h.parse_html_table(table, skiprows=1)]
    # Each table ends with an empty row.
    return [row for row in rows if any(value is not None for value in row.values())]


def build_person_from_family_row(
    context: Context, row: dict[str, str | None]
) -> Entity:
    first_name = row.pop("ime")
    last_name = row.pop("prezime")
    # We're dealing with: '-/-'
    maiden_name = (row.pop("rodeno_prezime") or "").strip("-").strip("/") or None
    # If the maiden name is actually a birthdate (i.e. only digits and dots),
    # treat it as the date of birth and clear the maiden name field
    dob = None
    if maiden_name is not None and re.fullmatch(r"[.\d]+", maiden_name):
        dob, maiden_name = maiden_name, None

    city = row.pop("mjesto")
    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, city)
    entity.add("citizenship", (row.pop("drzavljanstvo") or "").split(","))
    h.apply_name(
        entity, first_name=first_name, last_name=last_name, maiden_name=maiden_name
    )
    h.apply_date(entity, "birthDate", dob)
    residence = row.pop("boraviste")
    # The residence often repeats the city.
    if residence is not None and city is not None:
        if residence.casefold() == city.casefold():
            residence = None
    address = h.make_address(context, city=city, place=residence, lang="cnr")
    h.copy_address(entity, address)
    # The family relationship, used by the caller.
    context.audit_data(row, ["srodstvo"])
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
        context.emit(relative)

        rel = context.make("Family")
        rel.id = context.make_id(person.id, relationship, relative.id)
        rel.add("relationship", relationship)
        rel.add("person", person.id)
        rel.add("relative", relative.id)
        context.emit(rel)


def crawl_function(
    context: Context,
    person: Entity,
    row: dict[str, str | None],
    filing_date: str,
) -> bool:
    """Emit a function listed in a filing. Returns true if it is a national PEP."""
    function = row.pop("naziv_funkcije")
    organization = row.pop("organ_vlasti_gdje_se_obavlja_funkcija")
    if function is None or organization is None:
        context.log.warning(
            "Function without a name or organisation",
            person=person.id,
            function=function,
            organization=organization,
        )
        return False
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
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return False

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
        declaration_date=filing_date,
        # A filing is a snapshot of the functions held when it was filed.
        no_end_implies_current=False,
        # Some functions are marked as ended without an end date.
        status=OccupancyStatus.ENDED if ended == "X" and end_date is None else None,
    )
    if occupancy is None:
        return False

    context.emit(position)
    context.emit(occupancy)
    context.emit(person)
    return "gov.national" in categorisation.topics


def crawl_person(context: Context, person_data: dict[str, Any]) -> None:
    full_name = person_data.pop("imeIPrezime")
    filings: list[dict[str, Any]] = person_data.pop("izvjestajImovine")
    function_labels: list[str] = person_data.pop("nazivFunkcije")

    filing_date: str | None = None
    family_rows: list[dict[str, str | None]] = []
    function_rows: list[dict[str, str | None]] = []
    if filings:
        # A person can file more than once on the same day.
        latest = max(filings, key=lambda f: (f["datum"], f["barcode"]))
        filing_date = latest["datum"]
        doc = fetch_filing(context, latest["barcode"])
        # Without the report, the index labels below stand in for the functions.
        if doc is not None:
            family_rows = read_section(doc, "2.2 Članovi porodice")
            function_rows = read_section(doc, "2.4 Javna funkcija")

    # The official is listed among their family as "Funkcioner".
    officials = [row for row in family_rows if row.get("srodstvo") == "Funkcioner"]
    relatives = [row for row in family_rows if row.get("srodstvo") != "Funkcioner"]
    if len(officials) > 1:
        context.log.warning("Multiple officials in a filing", name=full_name)

    if officials:
        person = build_person_from_family_row(context, officials[0])
        # The filing can spell the name differently from the index, e.g. in
        # Cyrillic, without diacritics or with a married name.
        h.apply_name(person, full_name)
    else:
        person = context.make("Person")
        # TODO(Leon Handreke): I think we should be passing *function_labels here,
        # but that would mean a re-key.
        person.id = context.make_id(full_name, sorted(function_labels))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
        h.apply_name(person, full_name)
    person.add("citizenship", "me")

    is_national = False
    if function_rows:
        assert filing_date is not None
        for row in function_rows:
            if crawl_function(context, person, row, filing_date):
                is_national = True
    else:
        # Fall back to the functions the index lists.
        for label in function_labels:
            position = h.make_position(context, label, country="me")
            categorisation = categorise(context, position, default_is_pep=True)
            if not categorisation.is_pep:
                continue

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                categorisation=categorisation,
                declaration_date=filing_date,
                no_end_implies_current=False,
                # The index lists some people without any filing. It lists them
                # because they hold the function and owe a filing, so keep them
                # without a date to age out from.
                status=OccupancyStatus.UNKNOWN if filing_date is None else None,
            )
            if occupancy is None:
                continue

            context.emit(position)
            context.emit(occupancy)
            context.emit(person)
            if "gov.national" in categorisation.topics:
                is_national = True

    if is_national:
        crawl_relatives(context, person, relatives)


def crawl(context: Context) -> None:
    page = 0
    while True:
        # The API serves at most 2000 people per page.
        people = context.fetch_json(f"{context.data_url}?page={page}&size=2000")
        if not people:
            break
        for person in people:
            crawl_person(context, person)
        page += 1
