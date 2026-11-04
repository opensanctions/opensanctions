import gzip
import shutil
from datetime import date, timedelta
from itertools import chain
from pathlib import Path
from typing import Any
from collections.abc import Generator, Iterable, Iterator

import orjson
from requests.exceptions import HTTPError

from zavod import Context, Entity, settings
from zavod import helpers as h
from zavod.archive import (
    backfill_artifact,
    dataset_artifact_path,
    get_last_successful_version,
)

# The previous run's extracts, carried from run to run so that a run only
# refetches the numbers named in the court bulletin since then. Line one is a
# header with `bulletin_date`, `max_krs` and `full_scan`, each further line one
# KRS number with `krs`, `rejestr`, `fetched` and `odpis`, sorted by number.
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
        if odpis is not None:
            header = odpis["odpis"]["naglowekA"]
            assert header["numerKRS"] == f"{krs:010d}", (krs, header)
            assert header["rejestr"] == f"Rej{rejestr}", (krs, header)
        return {
            "krs": krs,
            "rejestr": rejestr,
            "fetched": settings.RUN_DATE,
            "odpis": odpis,
        }
    return None


def probe(
    context: Context, start: int, stop_after: int, reach: int
) -> Generator[Record, None, None]:
    """Fetch every number from `start` upward, past `reach`, until a long run of
    numbers that are in neither register."""
    misses = 0
    krs = start
    while misses < stop_after or krs <= reach:
        record = fetch_record(context, krs, refresh=False)
        if record is None:
            misses += 1
        else:
            misses = 0
            yield record
        if krs % PROGRESS_EVERY == 0:
            context.log.info("Probing KRS numbers", krs=krs)
        krs += 1


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


def clean_regon(regon: str | None) -> str | None:
    # The nine-digit REGON of a legal entity is padded with zeros to the
    # fourteen digits of a local unit's number.
    if regon is not None and len(regon) == 14 and regon.endswith("00000"):
        return regon[:9]
    return regon


def make_person(context: Context, krs: str, organ: str, item: Data) -> Entity:
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
    context.audit_data(ident)

    person = context.make("Person")
    person.id = context.make_id(krs, organ, last_name, given, middle, pesel, birth_date)
    h.apply_name(person, first_name=given, middle_name=middle, last_name=last_name)
    h.apply_date(person, "birthDate", birth_date)
    context.emit(person)
    return person


def make_legal_entity(context: Context, krs: str, organ: str, item: Data) -> Entity:
    name = item.pop("nazwa")
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
    entity.add("name", name)
    entity.add("registrationNumber", regon)
    entity.add("taxNumber", nip)
    context.emit(entity)
    return entity


def make_party(context: Context, krs: str, organ: str, item: Data) -> Entity:
    if "nazwisko" in item:
        return make_person(context, krs, organ, item)
    return make_legal_entity(context, krs, organ, item)


def crawl_owners(
    context: Context, company: Entity, krs: str, section: str, items: list[Data]
) -> None:
    for item in items:
        owner = make_party(context, krs, section, item)
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
                "wysokoscSumyKomandytowej",
            ],
        )
        context.emit(ownership)


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
        role = organ if role_key is None else item.pop(role_key, None) or organ
        directorship = context.make("Directorship")
        directorship.id = context.make_id(krs, organ, director.id, role)
        directorship.add("director", director)
        directorship.add("organization", company)
        directorship.add("role", role)
        context.audit_data(item, ignore=["czyZawieszona"])
        context.emit(directorship)


def make_address(context: Context, seat: Data) -> str:
    address = seat.pop("adres")
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
    company.add("name", subject.pop("nazwa"))
    company.add("legalForm", legal_form)
    company.add("jurisdiction", "pl")
    company.add("registrationNumber", krs)
    ids = subject.pop("identyfikatory", {})
    company.add("registrationNumber", clean_regon(ids.pop("regon", None)))
    company.add("taxNumber", ids.pop("nip", None))
    context.audit_data(ids, ignore=["statusNip", "dataDecyzji"])
    # An entity carried over from an earlier register existed before its
    # entry into the KRS.
    if subject.pop("daneOWczesniejszejRejestracji", None) is None:
        h.apply_date(company, "incorporationDate", header["dataRejestracjiWKRS"])
    context.audit_data(
        subject,
        ignore=[
            "czyPosiadaStatusOPP",
            "czyProwadziDzialalnoscZInnymiPodmiotami",
            "innyRejestr",
            "nazwaFormaPrawnaPrzedsiebiorcyZagranicznego",
        ],
    )

    company.add("address", make_address(context, seat))
    # The email field sometimes holds the website instead.
    email = seat.pop("adresPocztyElektronicznej", None)
    if email is not None and "@" in email:
        company.add("email", email)
    website = seat.pop("adresStronyInternetowej", None)
    if website is not None:
        company.add("website", h.multi_split(website, [";"]))
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
        if amount is not None:
            value = amount["wartosc"].replace(",", ".")
            company.add("capital", f"{value} {amount['waluta']}")

    for section in ("wspolnicySpzoo", "jedynyAkcjonariusz", "wspolnicyPartnerzy"):
        crawl_owners(context, company, krs, section, dzial1.pop(section, []))
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
        ],
    )

    dzial2 = dane.pop("dzial2", {})
    board = dzial2.pop("reprezentacja", None)
    if board is not None:
        organ = board.pop("nazwaOrganu")
        members = board.pop("sklad", [])
        crawl_directors(context, company, krs, organ, members, "funkcjaWOrganie")
        context.audit_data(board, ignore=["sposobReprezentacji"])
    for supervisory in dzial2.pop("organNadzoru", []):
        organ = supervisory.pop("nazwa")
        members = supervisory.pop("sklad", [])
        crawl_directors(context, company, krs, organ, members, None)
        context.audit_data(supervisory)
    proxies = dzial2.pop("prokurenci", [])
    crawl_directors(context, company, krs, "prokurenci", proxies, "rodzajProkury")
    context.audit_data(
        dzial2,
        ignore=["osobyReprezentujacePZ", "reprezentacjaIBIGBPPSPZOZ", "pelnomocnicy"],
    )

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
    full_scan_days: int = context.dataset.config["full_scan_days"]
    stop_after: int = context.dataset.config["stop_after_misses"]
    today = settings.RUN_TIME.date()
    yesterday = today - timedelta(days=1)

    records: Iterable[Record]
    full_scan = today
    incremental = False
    previous = load_previous(context)
    if previous is not None:
        old_header, old_records = previous
        last_full_scan = date.fromisoformat(old_header["full_scan"])
        if last_full_scan >= today - timedelta(days=full_scan_days):
            full_scan = last_full_scan
            incremental = True
    if not incremental:
        context.log.info("Running a full scan of the register")
        records = probe(context, 1, stop_after, 0)
    else:
        bulletin_date = date.fromisoformat(old_header["bulletin_date"])
        max_krs: int = old_header["max_krs"]
        changed = fetch_bulletins(context, bulletin_date, yesterday)
        context.log.info(
            "Running an incremental update",
            since=bulletin_date.isoformat(),
            changed=len(changed),
        )
        known = sorted(krs for krs in changed if krs <= max_krs)
        reach = max(changed, default=0)
        records = chain(
            merge(context, old_records, known),
            probe(context, max_krs + 1, stop_after, reach),
        )

    path = dataset_artifact_path(context.dataset.name, context.version, SNAPSHOT)
    writer = SnapshotWriter(path)
    for record in records:
        writer.write(record)
        if record["odpis"] is not None:
            crawl_extract(context, record)
    # The days of this run are replayed from the bulletin by the next one.
    writer.close(
        {
            "bulletin_date": yesterday.isoformat(),
            "max_krs": writer.max_krs,
            "full_scan": full_scan.isoformat(),
        }
    )
