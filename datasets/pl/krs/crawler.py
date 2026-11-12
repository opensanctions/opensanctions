import gzip
import re
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from collections.abc import Generator, Iterable, Iterator

import orjson
from followthemoney import registry
from requests.exceptions import HTTPError

from zavod import Context, Entity, settings
from zavod import helpers as h
from zavod.archive import (
    backfill_artifact,
    dataset_artifact_path,
    get_last_successful_version,
)

# The previous run's extracts, carried from run to run so that a run only
# refetches the numbers named in the court bulletin since then, plus a batch of
# numbers from a cursor that cycles through the register. Line one is a header
# with `bulletin_date`, `max_krs` and `cursor`, each further line one KRS number
# with `krs`, `rejestr`, `fetched` and `odpis`, sorted by number.
SNAPSHOT = "krs_snapshot.jsonl.gz"
EXTRACT_URL = "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs:010d}"
BULLETIN_URL = "https://api-krs.ms.gov.pl/api/Krs/Biuletyn/{day}"
# Each number lives in one of the register of entrepreneurs (P) and the
# register of associations (S).
REGISTERS = ("P", "S")
# The KRS number given for a shareholder from outside the register, such as a
# foreign company.
NO_KRS = "0000000000"
PROGRESS_EVERY = 10_000

Record = dict[str, Any]
Data = dict[str, Any]


def company_id(krs: str) -> str:
    return f"oc-companies-pl-{krs}"


def fetch_record(context: Context, krs: int, refresh: bool) -> Record | None:
    """Fetch the current extract of a KRS number from whichever register has it.

    A struck-off entity yields a record with `odpis` set to None, a number that
    is not in either register yields None.
    """
    url = EXTRACT_URL.format(krs=krs)
    struck_off: Record | None = None
    for rejestr in REGISTERS:
        params = {"rejestr": rejestr, "format": "json"}
        if refresh:
            context.clear_url(url, params=params)
        try:
            # A struck-off entity answers with an empty 204, which decodes to None.
            odpis = context.fetch_json(url, params=params, cache_days=7)
        except HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                continue
            raise
        record = {
            "krs": krs,
            "rejestr": rejestr,
            "fetched": settings.RUN_DATE,
            "odpis": odpis,
        }
        if odpis is not None:
            header = odpis["odpis"]["naglowekA"]
            assert header["numerKRS"] == f"{krs:010d}", (krs, header)
            # Some numbers answer with the other register's extract.
            assert header["rejestr"] in ("RejP", "RejS"), (krs, header)
            record["rejestr"] = header["rejestr"].removeprefix("Rej")
            return record
        # Some numbers answer empty in one register while the other holds
        # their extract, so an empty answer is final only once both are asked.
        if struck_off is None:
            struck_off = record
    return struck_off


class Window:
    """Fetch a batch of numbers from the cursor, stopping early at the end of
    the register: a long run of misses past `reach`, the highest number known
    to exist."""

    def __init__(self, start: int, size: int, reach: int, stop_after: int) -> None:
        self.start = start
        self.size = size
        self.reach = reach
        self.stop_after = stop_after
        self.stop = start
        self.wrapped = False

    def fetch(self, context: Context) -> Generator[Record, None, None]:
        misses = 0
        krs = self.start
        while krs < self.start + self.size:
            if misses >= self.stop_after and krs > self.reach:
                self.wrapped = True
                break
            record = fetch_record(context, krs, refresh=False)
            if record is None:
                misses += 1
            else:
                misses = 0
                yield record
            if krs % PROGRESS_EVERY == 0:
                context.log.info("Scanning KRS numbers", krs=krs)
            krs += 1
        self.stop = krs


def splice(
    records: Iterator[Record], window: Window, context: Context
) -> Generator[Record, None, None]:
    """Replace the records in the window's range with fresh extracts."""
    record = next(records, None)
    while record is not None and record["krs"] < window.start:
        yield record
        record = next(records, None)
    yield from window.fetch(context)
    while record is not None and record["krs"] < window.stop:
        record = next(records, None)
    if record is not None:
        yield record
        yield from records


def merge(
    context: Context, previous: Iterable[Record], changed: list[int]
) -> Generator[Record, None, None]:
    """Merge the previous snapshot with fresh extracts of the changed numbers."""
    pending = iter(changed)
    krs = next(pending, None)
    for old in previous:
        while krs is not None and krs < old["krs"]:
            record = fetch_record(context, krs, refresh=True)
            if record is not None:
                yield record
            krs = next(pending, None)
        if krs is not None and krs == old["krs"]:
            record = fetch_record(context, krs, refresh=True)
            if record is None:
                context.log.info("KRS number no longer in the register", krs=krs)
            else:
                yield record
            krs = next(pending, None)
        else:
            yield old
    while krs is not None:
        record = fetch_record(context, krs, refresh=True)
        if record is not None:
            yield record
        krs = next(pending, None)


def fetch_bulletins(context: Context, after: date, until: date) -> set[int]:
    """Collect the KRS numbers with an entry in the court bulletin on any day
    after `after`, up to and including `until`."""
    numbers: set[int] = set()
    day = after + timedelta(days=1)
    while day <= until:
        url = BULLETIN_URL.format(day=day.isoformat())
        listed = context.fetch_json(url, cache_days=7)
        assert isinstance(listed, list), (url, listed)
        for value in listed:
            assert isinstance(value, str) and value.isdigit(), (url, value)
            numbers.add(int(value))
        day += timedelta(days=1)
    return numbers


def read_snapshot(path: Path) -> tuple[Data, Iterator[Record]]:
    fh = gzip.open(path, "rb")
    header = orjson.loads(fh.readline())

    def records() -> Generator[Record, None, None]:
        with fh:
            for line in fh:
                yield orjson.loads(line)

    return header, records()


def load_previous(context: Context) -> tuple[Data, Iterator[Record]] | None:
    version = get_last_successful_version(context.dataset.name)
    if version is None:
        return None
    path = backfill_artifact(context.dataset.name, version, SNAPSHOT)
    if path is None:
        return None
    return read_snapshot(path)


class SnapshotWriter:
    """Write the records of a run to the snapshot, then prepend the header once
    the highest KRS number is known."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.body_path = path.with_name(f"{path.name}.body")
        self.body = gzip.open(self.body_path, "wb")
        self.max_krs = 0

    def write(self, record: Record) -> None:
        assert record["krs"] > self.max_krs, (record["krs"], self.max_krs)
        self.max_krs = record["krs"]
        self.body.write(orjson.dumps(record))
        self.body.write(b"\n")

    def close(self, header: Data) -> None:
        self.body.close()
        # Concatenated gzip members read back as a single stream.
        with open(self.path, "wb") as out:
            out.write(gzip.compress(orjson.dumps(header) + b"\n"))
            with open(self.body_path, "rb") as body:
                shutil.copyfileobj(body, out)
        self.body_path.unlink()


def clean(value: str | None) -> str | None:
    if value is None or value.strip() in ("", "-"):
        return None
    return value


def unmask(value: str | None) -> str | None:
    """Drop a name part that the register masked down to symbols, such as `*`
    or `#####`."""
    if value is None or not any(c.isalpha() for c in value):
        return None
    return value


def clean_name(name: str) -> str:
    """Swap the angle brackets that some names use as quotation marks for
    guillemets, which do not read as markup."""
    return name.replace("<", "‹").replace(">", "›")


def clean_regon(regon: str | None) -> str | None:
    # The nine-digit REGON of a legal entity is padded with zeros to the
    # fourteen digits of a local unit's number.
    if regon is not None and len(regon) == 14 and regon.endswith("00000"):
        return regon[:9]
    return regon


def make_person(context: Context, krs: str, organ: str, item: Data) -> Entity | None:
    """Make a stub for a natural person, whose name and PESEL the register
    masks, local to the entity they are listed with."""
    surname = item.pop("nazwisko")
    first = surname.pop("nazwiskoICzlon")
    second = surname.pop("nazwiskoIICzlon", None)
    context.audit_data(surname)
    last_name = first if second is None else f"{first}-{second}"
    names = item.pop("imiona")
    given = names.pop("imie", None)
    middle = names.pop("imieDrugie", None)
    context.audit_data(names)
    ident = item.pop("identyfikator", {})
    pesel = ident.pop("pesel", None)
    birth_date = ident.pop("dataUrodzenia", None)
    national_id = ident.pop("KrajowyNumerIdentyfikacyjny", None)
    context.audit_data(ident)

    person = context.make("Person")
    person.id = context.make_id(krs, organ, last_name, given, middle, pesel, birth_date)
    parts = [p for p in (unmask(first), unmask(second)) if p is not None]
    last_name = "-".join(parts) if parts else None
    given = unmask(given)
    middle = unmask(middle)
    if last_name is None and given is None:
        context.log.info("Skipping a fully masked person", krs=krs, organ=organ)
        return None
    h.apply_name(person, first_name=given, middle_name=middle, last_name=last_name)
    h.apply_date(person, "birthDate", birth_date)
    # Some entries hold the birth date in the national identifier field.
    if national_id is not None and re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", national_id):
        if birth_date is None:
            h.apply_date(person, "birthDate", national_id)
    else:
        person.add("idNumber", national_id)
    context.emit(person)
    return person


def make_legal_entity(
    context: Context, krs: str, organ: str, item: Data
) -> Entity | None:
    name = clean_name(item.pop("nazwa"))
    assert not item.pop("imiona", None), item
    other_krs = item.pop("krs", {}).pop("krs", None)
    ident = item.pop("identyfikator", {})
    regon = clean_regon(ident.pop("regon", None))
    nip = ident.pop("nip", None)
    context.audit_data(ident)

    entity = context.make("LegalEntity")
    if other_krs is not None and other_krs != NO_KRS:
        entity.id = company_id(other_krs)
        entity.add("registrationNumber", other_krs)
    else:
        entity.id = context.make_id(krs, organ, name, regon)
    entity.add("name", unmask(name))
    entity.add("registrationNumber", regon)
    entity.add("taxNumber", nip)
    if not entity.properties:
        context.log.info("Skipping a fully masked legal entity", krs=krs, name=name)
        return None
    context.emit(entity)
    return entity


def make_party(context: Context, krs: str, organ: str, item: Data) -> Entity | None:
    if "nazwisko" in item and item["nazwisko"] != {}:
        return make_person(context, krs, organ, item)
    if "nazwa" in item:
        return make_legal_entity(context, krs, organ, item)
    # Some entries list a role, but no one holding it, or a person with empty
    # name blocks.
    context.log.info("Skipping a party without a name", krs=krs, item=item)
    return None


def crawl_owners(
    context: Context, company: Entity, krs: str, section: str, items: list[Data]
) -> None:
    for item in items:
        owner = make_party(context, krs, section, item)
        if owner is None:
            continue
        ownership = context.make("Ownership")
        ownership.id = context.make_id(krs, section, owner.id)
        ownership.add("owner", owner)
        ownership.add("asset", company)
        ownership.add("summary", item.pop("posiadaneUdzialy", None))
        context.audit_data(
            item,
            ignore=[
                "czyPosiadaCaloscUdzialow",
                "czyPosiadaCaloscAkcji",
                "czyJestKomandytariuszem",
                "czyMaOgraniczonaZdolnoscDoCzynnosciPrawnych",
                "czyPonosiOdpowiedzialnoscKSHArt95_2",
                "czyPowstalaRozdzielnoscMajatkowa",
                "czyPozostajeWZwiazkuMalzenskim",
                "czyZawartaMalzenskaUmowaMajatkowa",
                "wartoscWkladuWspolnikaWUmowie",
                "wkladWspolnikaWUmowieCzyNiepieniezny",
                "wkladyWniesione",
                "wkladyZwrocone",
                "wysokoscSumyKomandytowej",
            ],
        )
        context.emit(ownership)


def crawl_group(context: Context, company: Entity, krs: str, items: list[Data]) -> None:
    """Link a company to its dominant company or its subsidiaries in a group of
    companies, which the parent controls by votes or board appointments rather
    than necessarily by owning it."""
    section = "wzmiankaOUczestnicwieWGrupieSpolek"
    for item in items:
        kind = item.pop("czyPodmiotZaleznyDominujacy")
        if kind == "SPÓŁKA ZALEŻNA":
            is_parent = False
            members = item.pop("podmiotDominujacy")
        elif kind == "PODMIOT DOMINUJĄCY":
            is_parent = True
            members = item.pop("spolkiZalezne")
        else:
            context.log.warning(
                "Unknown role in a group of companies", krs=krs, kind=kind
            )
            continue
        for member_item in members:
            member = make_legal_entity(context, krs, section, member_item)
            context.audit_data(member_item)
            if member is None:
                continue
            controller, controlled = (
                (company, member) if is_parent else (member, company)
            )
            control = context.make("Control")
            # Both the parent and the subsidiary list the link, so the ID must
            # not depend on which record it is read from.
            control.id = context.make_id(section, controller.id, controlled.id)
            control.add("controller", controller)
            control.add("controlled", controlled)
            context.emit(control)
        context.audit_data(item, ignore=["numerWzmianki"])


def crawl_directors(
    context: Context,
    company: Entity,
    krs: str,
    organ: str,
    items: list[Data],
    role_key: str | None,
) -> None:
    for item in items:
        director = make_party(context, krs, organ, item)
        if director is None:
            continue
        role = organ if role_key is None else item.pop(role_key, None) or organ
        directorship = context.make("Directorship")
        directorship.id = context.make_id(krs, organ, director.id, role)
        directorship.add("director", director)
        directorship.add("organization", company)
        directorship.add("role", role)
        context.audit_data(item, ignore=["czyZawieszona", "dataZawieszeniaDo"])
        context.emit(directorship)


def make_address(context: Context, seat: Data) -> str | None:
    address = seat.pop("adres")
    # Some old entries record no address beyond the country.
    if address.keys() == {"kraj"}:
        return None
    country = address.pop("kraj")
    house = clean(address.pop("nrDomu", None))
    flat = clean(address.pop("nrLokalu", None))
    if house is not None and flat is not None:
        house = f"{house}/{flat}"
    full = h.format_address(
        street=clean(address.pop("ulica", None)),
        house_number=house,
        postal_code=address.pop("kodPocztowy", None),
        city=address.pop("miejscowosc"),
        country=country,
        country_code="pl" if country == "POLSKA" else None,
    )
    context.audit_data(address, ignore=["poczta"])
    return full


def add_pkd(entity: Entity, items: list[Data]) -> None:
    for item in items:
        # Some entries are empty or hold only a free-text description.
        if "kodDzial" not in item:
            continue
        parts = [item["kodDzial"], item.get("kodKlasa"), item.get("kodPodklasa")]
        entity.add("classification", ".".join(p for p in parts if p))


def crawl_extract(context: Context, record: Record) -> None:
    krs = f"{record['krs']:010d}"
    odpis = record["odpis"]["odpis"]
    header = odpis.pop("naglowekA")
    dane = odpis.pop("dane")

    dzial1 = dane.pop("dzial1")
    subject = dzial1.pop("danePodmiotu", None)
    if subject is None:
        subject = dzial1.pop("danePodmiotuZagranicznego")
    seat = dzial1.pop("siedzibaIAdres", None)
    if seat is None:
        seat = dzial1.pop("siedzibaIAdresPodmiotuZagranicznego")

    legal_form = subject.pop("formaPrawna")
    # An unmapped form must not crash a scan that runs for days.
    schema = context.lookup_value(
        "legal_form", legal_form, "LegalEntity", warn_unmatched=True
    )
    assert schema is not None
    company = context.make(schema)
    company.id = company_id(krs)
    company.add("name", clean_name(subject.pop("nazwa")))
    company.add("legalForm", legal_form)
    company.add("jurisdiction", "pl")
    company.add("registrationNumber", krs)
    ids = subject.pop("identyfikatory", {})
    company.add("registrationNumber", clean_regon(ids.pop("regon", None)))
    company.add("taxNumber", ids.pop("nip", None))
    context.audit_data(ids, ignore=["statusNip", "dataDecyzji"])
    # An entity carried over from an earlier register existed before its
    # entry into the KRS.
    previous = subject.pop("daneOWczesniejszejRejestracji", None)
    if previous is None or previous == {}:
        h.apply_date(company, "incorporationDate", header["dataRejestracjiWKRS"])
    context.audit_data(
        subject,
        ignore=[
            "czyPosiadaStatusOPP",
            "czyProwadziDzialalnoscZInnymiPodmiotami",
            "daneOPoprzedniejRejestracjiEZIG",
            "innyRejestr",
            "nazwaFormaPrawnaPrzedsiebiorcyZagranicznego",
            "notyfikacjeBRIS",
            "prawoPanstwa",
        ],
    )

    company.add("address", make_address(context, seat))
    # Contact fields are often malformed beyond repair, or the email field
    # holds the website, so parts that do not validate are dropped quietly.
    email = seat.pop("adresPocztyElektronicznej", None)
    if email is not None:
        emails = h.multi_split(email, [",", ";", " "])
        company.add("email", [e for e in emails if registry.email.validate(e)])
    website = seat.pop("adresStronyInternetowej", None)
    if website is not None:
        websites = h.multi_split(website, [";", ",", " "])
        company.add("website", [w for w in websites if registry.url.validate(w)])
    context.audit_data(
        seat,
        ignore=[
            "siedziba",
            "adresDoDoreczenElektronicznychWpisanyDoBAE",
            "siedzibaIAdresZakladuGlownego",
        ],
    )

    for key, amount_key in (
        ("kapital", "wysokoscKapitaluZakladowego"),
        ("kapitalPSA", "wysokoscKapitaluAkcyjnego"),
    ):
        amount = dzial1.pop(key, {}).get(amount_key)
        # An unmapped legal form falls back to LegalEntity, which has no capital.
        if amount is not None and company.schema.get("capital") is not None:
            value = amount["wartosc"].replace(",", ".")
            company.add("capital", f"{value} {amount['waluta']}")

    for section in ("wspolnicySpzoo", "jedynyAkcjonariusz", "wspolnicyPartnerzy"):
        crawl_owners(context, company, krs, section, dzial1.pop(section, []))
    group = dzial1.pop("wzmiankaOUczestnicwieWGrupieSpolek", [])
    crawl_group(context, company, krs, group)
    context.audit_data(
        dzial1,
        ignore=[
            "umowaStatut",
            "pozostaleInformacje",
            "sposobPowstaniaPodmiotu",
            "emisjeAkcji",
            "jednostkiTerenoweOddzialy",
            "wzmiankaOUpowaznieniuDoEmisjiWarrantowSubskrypcyjnych",
            "wzmiankaOPodjeciuUchwalyOEmisjachObligacjiZamiennych",
            "komitetZalozycielski",
            "organSprawujacyNadzor",
            "organPodmiotZalozycielskiMinisterNadzorujacy",
            "informacjeOpisujaceTowarzystwo",
            "czlonkowieEZIG",
        ],
    )

    dzial2 = dane.pop("dzial2", {})
    board = dzial2.pop("reprezentacja", None)
    if board is not None:
        organ = board.pop("nazwaOrganu", None) or "reprezentacja"
        members = board.pop("sklad", [])
        crawl_directors(context, company, krs, organ, members, "funkcjaWOrganie")
        context.audit_data(board, ignore=["sposobReprezentacji"])
    for supervisory in dzial2.pop("organNadzoru", []):
        # Some associations list a supervisory organ without any details.
        if supervisory == {}:
            continue
        organ = supervisory.pop("nazwa", None) or "organNadzoru"
        members = supervisory.pop("sklad", [])
        crawl_directors(context, company, krs, organ, members, None)
        context.audit_data(supervisory)
    proxies = dzial2.pop("prokurenci", [])
    crawl_directors(context, company, krs, "prokurenci", proxies, "rodzajProkury")
    # The people who represent a foreign company in its Polish branch.
    branch_reps = dzial2.pop("osobyReprezentujacePZ", [])
    crawl_directors(
        context, company, krs, "osobyReprezentujacePZ", branch_reps, "funkcjaWOddziale"
    )
    context.audit_data(dzial2, ignore=["reprezentacjaIBIGBPPSPZOZ", "pelnomocnicy"])

    dzial3 = dane.pop("dzial3", {})
    activity = dzial3.pop("przedmiotDzialalnosci", {})
    add_pkd(company, activity.pop("przedmiotPrzewazajacejDzialalnosci", []))
    add_pkd(company, activity.pop("przedmiotPozostalejDzialalnosci", []))
    context.audit_data(activity)
    activity_opp = dzial3.pop("przedmiotDzialalnosciOPP", {})
    add_pkd(company, activity_opp.pop("nieodplatnyPkd", []))
    add_pkd(company, activity_opp.pop("odplatnyPkd", []))
    context.audit_data(activity_opp)
    context.audit_data(
        dzial3,
        ignore=[
            "wzmiankiOZlozonychDokumentach",
            "sprawozdaniaGrupyKapitalowej",
            "informacjaODniuKonczacymRokObrotowy",
            "celDzialaniaOrganizacji",
        ],
    )
    context.audit_data(dane, ignore=["dzial4", "dzial5", "dzial6"])
    context.emit(company)


def crawl(context: Context) -> None:
    batch_size: int = context.dataset.config["batch_size"]
    stop_after: int = context.dataset.config["stop_after_misses"]
    yesterday = settings.RUN_TIME.date() - timedelta(days=1)

    previous = load_previous(context)
    old_records: Iterator[Record] = iter(())
    # A first run reads one day of the bulletin, to learn the highest number.
    bulletin_date = yesterday - timedelta(days=1)
    max_krs = 0
    cursor = 1
    if previous is not None:
        old_header, old_records = previous
        bulletin_date = date.fromisoformat(old_header["bulletin_date"])
        max_krs = old_header["max_krs"]
        cursor = old_header["cursor"]

    changed = fetch_bulletins(context, bulletin_date, yesterday)
    window = Window(cursor, batch_size, max([max_krs, *changed]), stop_after)
    known = sorted(
        krs
        for krs in changed
        if krs <= max_krs and not cursor <= krs < cursor + batch_size
    )
    context.log.info(
        "Updating the register snapshot",
        since=bulletin_date.isoformat(),
        changed=len(known),
        cursor=cursor,
    )
    records = splice(merge(context, old_records, known), window, context)

    path = dataset_artifact_path(context.dataset.name, context.version, SNAPSHOT)
    writer = SnapshotWriter(path)
    for record in records:
        writer.write(record)
        if record["odpis"] is not None:
            crawl_extract(context, record)
    if window.wrapped:
        context.log.info("Reached the end of the register", krs=window.stop)
    # The days of this run are replayed from the bulletin by the next one.
    writer.close(
        {
            "bulletin_date": yesterday.isoformat(),
            "max_krs": writer.max_krs,
            "cursor": 1 if window.wrapped else window.stop,
        }
    )
