import csv
import io
import re
from datetime import date
from pathlib import Path

import requests
from zavod.shed.ojeu import cellar
from zavod.shed.ojeu.celex import eur_lex_url

from zavod import Context, Entity, settings
from zavod import helpers as h

DATA_DIR = Path(__file__).parent / "data"
# Entity columns holding a free-form source date rather than a plain value.
CSV_DATE_PROPS = frozenset({"birthDate", "incorporationDate"})
# Multi-valued name columns, reviewed alongside the scalar `name` column.
CSV_NAME_PROPS = ("alias", "weakAlias", "previousName")
# A name whose bracketed tail may be an abbreviation: one group, closing the value.
TRAILING_ABBREVIATION_RE = re.compile(r"^(?P<name>[^()]+?)\s*\((?P<abbr>[^()]+)\)$")
# The scripts an abbreviation is recognised in. Cyrillic short forms are a
# different shape ("АО «Казанский Вертолетный Завод»") and go to review instead.
LATIN_ABBREVIATION_RE = re.compile(r"[A-Za-z0-9 .,&/’'\-]+")


def split_cell(value: str) -> list[str]:
    """Decode a multi-valued CSV cell into its elements.

    Counterpart to `join_multi` in scripts/common.py: values are ";"-separated
    and a value containing the separator or a quote is CSV-quoted within the
    cell, so a naive split would corrupt it.
    """
    if not value:
        return []
    rows = list(csv.reader(io.StringIO(value), delimiter=";", skipinitialspace=True))
    if len(rows) != 1:
        raise ValueError(f"Multi-value cell contains a line break: {value!r}")
    return [element.strip() for element in rows[0] if element.strip()]


def split_trailing_abbreviation(value: str) -> tuple[str, str] | None:
    """Break "Really Long Factory Name (RLFN)" into its name and its acronym.

    The acts print an entity's acronym after its name in brackets, and
    transcription keeps the printed wording, so both arrive as one string on one
    property. Returns None unless the bracketed part is confidently an
    abbreviation rather than a disambiguator ("VTB Bank (Belarus)"), an editorial
    note ("(as previously listed)"), or a second name in its own right.
    """
    match = TRAILING_ABBREVIATION_RE.match(value)
    if match is None:
        return None
    name = match.group("name").strip()
    abbr = match.group("abbr").strip()
    if len(name.split()) < 2:
        return None
    if not 2 <= len(abbr) <= 20:
        return None
    if len(abbr) >= len(name) * 0.5:
        return None
    if LATIN_ABBREVIATION_RE.fullmatch(abbr) is None:
        return None
    letters = [char for char in abbr if char.isalpha()]
    if not letters or not letters[0].isupper():
        return None
    # An acronym is mostly capitals ("TsAGI", "VGTRK"); a short form is a piece
    # of the printed name ("Joint Stock Company Metallist Samara (Metallist
    # Samara)"). A place name is neither, which is what keeps the
    # disambiguating "(Kyrgyzstan)", "(Hamburg)" forms whole.
    mostly_capitals = sum(char.isupper() for char in letters) >= len(letters) * 0.5
    if not mostly_capitals and abbr.lower() not in name.lower():
        return None
    return name, abbr


def suggest_abbreviations(entity: Entity, names: h.Names) -> h.Names:
    """Move a printed trailing acronym out of each name value into `abbreviation`.

    Person parentheticals are notes rather than acronyms ("(nom de guerre)",
    "(as previously listed)"), and a schema without the property cannot carry
    the result, so both are returned untouched.
    """
    if entity.schema.is_a("Person") or entity.schema.get("abbreviation") is None:
        return names
    suggested = h.Names()
    for prop, values in names.as_langtexts():
        for value in values:
            split = split_trailing_abbreviation(value.text)
            if split is None:
                suggested.add(prop, value.text, lang=value.lang)
                continue
            name, abbr = split
            suggested.add(prop, name, lang=value.lang)
            suggested.add("abbreviation", abbr, lang=value.lang)
    return suggested


def crawl_csv_data(context: Context, path: Path) -> None:
    """Emit the designations transcribed into one reviewed CSV.

    Handles both file kinds specified in data/FORMAT.md, which differ only in
    their leading CELEX columns. Entity columns are named after the FtM
    property they populate, so a column the row's schema does not carry fails
    loudly here.
    """
    with open(path, encoding="utf-8") as infh:
        for row in csv.DictReader(infh):
            celex = row.pop("celex", None)
            if celex is None:
                # An amendment row keys on the framework act it amends, so the
                # designation keeps its ID once the consolidated snapshot
                # catches up; the URL names the act that made the change.
                celex = row.pop("amendedCelex")
                source_url = eur_lex_url(row.pop("amendmentCelex"))
            else:
                source_url = eur_lex_url(celex)
            # The annex locates the listing in the act, but the same entity
            # listed in two annexes is one designation under one program.
            row.pop("annex")
            record_id = row.pop("recordId")
            program_key = row.pop("programKey")
            measure = row.pop("measure")
            start_date = row.pop("startDate")
            reason = row.pop("reason")
            name = row.pop("name")

            entity = context.make(row.pop("schema"))
            entity.id = context.make_id(celex, record_id, name)
            entity.add("topics", "sanction")
            entity.add("sourceUrl", source_url)

            # Transcription only categorises a name where the act prints a label
            # saying so, so the whole name block is reviewed together and a human
            # decides the rest. The crawler proposes one categorisation of its
            # own, the printed acronym. While a review is pending, each string
            # stays on the property the source gave it.
            names = h.Names(name=name)
            for name_prop in CSV_NAME_PROPS:
                for value in split_cell(row.pop(name_prop)):
                    names.add(name_prop, value)
            # The proposal is built on top of the standard heuristics, not
            # instead of them: passing `suggested` skips check_names_regularity,
            # which is what applies the yml's name rules.
            is_irregular, regular = h.check_names_regularity(entity, names)
            h.apply_reviewed_names(
                context,
                entity,
                original=names,
                suggested=suggest_abbreviations(entity, regular),
                is_irregular=is_irregular,
            )

            for prop, cell in row.items():
                values = split_cell(cell)
                if not values:
                    continue
                if prop in CSV_DATE_PROPS:
                    h.apply_dates(entity, prop, values)
                else:
                    entity.add(prop, values)

            sanction = h.make_sanction(
                context,
                entity,
                key=program_key,
                program_key=program_key,
            )
            sanction.add("recordId", record_id)
            sanction.add("provisions", measure)
            sanction.add("reason", reason)
            sanction.add("sourceUrl", source_url)
            h.apply_date(sanction, "startDate", start_date)

            context.emit(entity)
            context.emit(sanction)


def consolidation_pins(context: Context) -> dict[str, str]:
    """Framework act to the consolidated version its snapshot was parsed from."""
    pins: dict[str, str] = context.dataset.config["consolidation"]
    return pins


def pin_date(pin: str) -> date:
    """The version date a consolidated CELEX names ('02012R0267-20260801')."""
    return date.fromisoformat(pin.rsplit("-", 1)[-1])


def crawl_csv_consolidated(context: Context) -> None:
    """Emit the reviewed snapshot of every pinned framework act."""
    pins = consolidation_pins(context)
    directory = DATA_DIR / "consolidated"
    unpinned = {path.stem for path in directory.glob("*.csv")} - set(pins)
    if unpinned:
        context.log.warning("Snapshot has no consolidation pin", celex=sorted(unpinned))
    for framework in sorted(pins):
        crawl_csv_data(context, directory / f"{framework}.csv")


def amendment_framework(path: Path) -> str:
    """The framework act whose annex an amendment file changes."""
    with open(path, encoding="utf-8") as infh:
        return next(csv.DictReader(infh))["amendedCelex"]


def crawl_csv_amendments(context: Context) -> None:
    """Emit each reviewed amendment a consolidated snapshot has not absorbed.

    An amendment is dropped only once the snapshot named as consolidating it is
    the one checked in, so an unproven handoff keeps the designations published
    rather than losing them between the two sources.
    """
    configured: dict[str, str] = context.dataset.config.get(
        "consolidated_amendments", {}
    )
    pins = consolidation_pins(context)
    paths = sorted((DATA_DIR / "amendments").glob("*.csv"))
    stale = set(configured) - {path.stem for path in paths}
    if stale:
        context.log.warning(
            "No amendment file for a consolidated CELEX", celex=sorted(stale)
        )
    for path in paths:
        consolidated = configured.get(path.stem)
        if consolidated is not None:
            framework = amendment_framework(path)
            pinned = pins.get(framework)
            if pinned == consolidated:
                context.log.info(
                    "Amendment consolidated, skipping",
                    celex=path.stem,
                    consolidated=consolidated,
                )
                continue
            context.log.warning(
                "Amendment consolidated into a snapshot we have not parsed",
                celex=path.stem,
                framework=framework,
                consolidated=consolidated,
                pinned=pinned,
            )
        crawl_csv_data(context, path)


def check_new_amendments(context: Context) -> None:
    """Warn about acts amending a tracked framework that no reviewed file covers.

    An act published after its framework's consolidation pin cannot be in the
    snapshot we parse, so until it is transcribed its designations are in
    neither input. The pin doubles as the discovery cursor: bumping it on the
    next snapshot clears every act it absorbed.
    """
    pinned = {
        celex: pin_date(pin) for celex, pin in consolidation_pins(context).items()
    }
    reviewed = {path.stem for path in (DATA_DIR / "amendments").glob("*.csv")}
    reviewed.update(context.dataset.config.get("reviewed_acts", []))
    client = cellar.CellarClient(context.http, context.cache)
    # A designation must not wait on a cache: discovery is always fetched fresh.
    acts = client.query_related_acts(
        sorted(pinned), date_from=min(pinned.values()), cache_days=None
    )
    for act in {act.celex: act for act in acts}.values():
        consolidated = pinned.get(act.framework_celex)
        if consolidated is None or act.document_date <= consolidated.isoformat():
            continue
        if act.celex in reviewed:
            continue
        context.log.warning(
            "Amending act has no reviewed transcription",
            celex=act.celex,
            framework=act.framework_celex,
            document_date=act.document_date,
            resource_type=act.resource_type,
            title=act.title,
            url=eur_lex_url(act.celex),
        )


def check_consolidation_pins(context: Context) -> None:
    """Warn when CELLAR has published a consolidation newer than our pin.

    A stale pin means the snapshot no longer reflects the framework, so
    designations the new version removed are still being emitted. Regenerate
    with the matching scripts/parse_*.py and bump the pin in the same commit.
    """
    pins = consolidation_pins(context)
    client = cellar.CellarClient(context.http, context.cache)
    try:
        published = client.query_consolidations(sorted(pins), cache_days=1)
    except requests.RequestException as exc:
        # Emission is already complete; a flaky endpoint must not fail the run.
        context.log.warning("Could not list consolidated versions", error=str(exc))
        return
    for framework, pin in sorted(pins.items()):
        versions = published.get(framework)
        if versions is None:
            context.log.warning(
                "CELLAR reports no consolidated version", framework=framework
            )
            continue
        latest = max(versions)
        if latest > pin:
            context.log.warning(
                "Newer consolidated version published",
                framework=framework,
                pinned=pin,
                latest=latest,
                url=eur_lex_url(latest),
            )
        elif latest < pin:
            # The pin names a version CELLAR does not offer: a typo, or withdrawn.
            context.log.warning(
                "Pinned consolidation is not published by CELLAR",
                framework=framework,
                pinned=pin,
                latest=latest,
            )


def crawl(context: Context) -> None:
    crawl_csv_consolidated(context)
    crawl_csv_amendments(context)

    # Discovery emits no entity and costs minutes of CELLAR queries, so
    # `zavod --debug crawl` skips it and iterating on the CSVs stays fast.
    if settings.DEBUG is False:
        check_new_amendments(context)
        check_consolidation_pins(context)

    # Warn rather than raise: the dataset keeps publishing the source wording
    # while the name review backlog is worked through.
    # FIXME: avoid triggering loads of re-runs while the initial review is in progress.
    # assert_all_accepted(context, raise_on_unaccepted=False)
