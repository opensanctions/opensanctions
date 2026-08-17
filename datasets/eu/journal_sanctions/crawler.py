import csv
import io
import re
from datetime import date, timedelta
from functools import cache
from pathlib import Path

import requests
from followthemoney.types import registry
from lxml import html
from nomenklatura.resolver import Linker
from normality import normalize
from rigour.ids.ogrn import OGRN
from zavod.integration import get_dataset_linker
from zavod.shed.ojeu import cellar
from zavod.shed.ojeu.celex import eur_lex_url
from zavod.shed.ojeu.celex import normalize as normalize_celex
from zavod.stateful.review import assert_all_accepted

from zavod import Context, Entity, settings
from zavod import helpers as h

# Some Russia-related entries are sourced from the consolidated regulation text.
SPECIAL_CASE_URL = (
    "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:02014R0833-20240625"
)
# year/number or number/year with optional suffix
FIRST_CODE_RE = re.compile(
    r"\b(?:No\s+)?(\d{1,4}/\d{1,4})(?:/[A-Z]{2,5})?\b", re.IGNORECASE
)
# Recent journal notices can appear before the consolidated act is refreshed.
CHECK_CONSOLIDATED_DATE = h.backdate(settings.RUN_TIME, timedelta(days=90))

# Canonical EU feeds a journal row can graduate into, keyed by entity ID prefix.
CANONICAL_FEEDS = {
    "eu-fsf-": "FSF XML",
    "eu-sancmap-": "EU Sanctions map",
    "eu-tb-": "EU Travel Bans",
}
# Presence in the travel bans list alone does not mean the designation reached the
# consolidated file, so those rows are reported but not proposed for removal.
RETIRE_FEEDS = frozenset({"eu-fsf-", "eu-sancmap-"})

# Sheet rows that can be deleted, mapped to their name for the end-of-run summary.
GC_ROWS: dict[int, str] = {}

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
# The longest string the FtM name type carries. The acts print an a.k.a. run as
# one comma-joined string and the contract keeps it whole, so a value this long
# is a list of names rather than a name.
NAME_MAX_LENGTH = registry.name.max_length


@cache
def extract_program_code(context: Context, source_url: str) -> str | None:
    """Fetch the EU act code (e.g. '267/2012') for a sanctions notice.

    Use this to link journal rows to their sanctions program. The code is taken
    from the notice title that names the amended or implemented framework act.
    """
    if SPECIAL_CASE_URL in source_url:
        return "833/2014"
    try:
        celex = normalize_celex(source_url)
    except ValueError:
        context.log.warning(f"Could not find CELEX in source URL: {source_url}")
        return None
    program_xpath = "//div[@class='eli-main-title']/p[@class='oj-doc-ti']"
    client = cellar.CellarClient(context.http, context.cache)
    expression = client.fetch_expression(celex, cache_days=365)
    doc = html.fromstring(expression.content)
    title_nodes = h.xpath_elements(doc, program_xpath)
    if len(title_nodes) == 0:
        context.log.warning(f"Could not find program for {source_url}")
        return None
    # The last title paragraph names the framework act.
    title = h.element_text(title_nodes[-1])
    match = FIRST_CODE_RE.search(title)
    if not match:
        context.log.warning(
            f"No EU codes found in program name: {title}",
            source_url=source_url,
        )
        return None
    return match.group(1)


@cache
def get_consolidated_act(context: Context, source_url: str) -> cellar.Act | None:
    """Resolve a notice URL to the latest consolidated version of its framework act.

    Use this when checking whether a journal row still appears in the current
    consolidated regulation. The CELLAR graph provides both the amended framework
    act and its consolidated CELEX family.
    """
    try:
        celex = normalize_celex(source_url)
    except ValueError:
        context.log.warning(f"Could not find CELEX in source URL: {source_url}")
        return None
    client = cellar.CellarClient(context.http, context.cache)
    act = client.query_act(celex, cache_days=1)
    frameworks = list(act.amends)
    if len(frameworks) == 0:
        context.log.warning(
            "Could not find framework act amended by source act",
            source_url=source_url,
        )
        return None
    if len(frameworks) > 1:
        context.log.warning(
            "Source act amends multiple framework acts",
            source_url=source_url,
            frameworks=frameworks,
        )
    if act.latest_consolidated is None:
        context.log.info(
            "No consolidated version found for framework act",
            source_url=source_url,
            frameworks=frameworks,
        )
        return None
    return act


def get_consolidated_text(context: Context, act: cellar.Act) -> str | None:
    """Fetch the full text of a consolidated EU regulation from CELLAR.

    Use this when the crawler needs the regulation body for name-presence checks.
    """
    celex = act.latest_consolidated
    assert celex is not None
    client = cellar.CellarClient(context.http, context.cache)
    expression = client.fetch_expression(celex, cache_days=1)
    doc = html.fromstring(expression.content)
    text = h.element_text(doc)
    if not text:
        context.log.warning("Could not extract regulation text", celex=celex)
        return None
    return text


@cache
def _law_normalized(context: Context, act: cellar.Act) -> str | None:
    text = get_consolidated_text(context, act)
    return normalize(text) if text is not None else None


@cache
def _law_ascii(context: Context, act: cellar.Act) -> str | None:
    text = get_consolidated_text(context, act)
    return normalize(text, ascii=True) if text is not None else None


def check_in_consolidated_act_text(
    context: Context, start_date: str, names: list[str], list_id: str, source_url: str
) -> None:
    """Warn if any name in `names` is absent from the consolidated regulation text.

    Use this to identify journal rows that likely disappeared from the current
    regulation. Names are checked with their source spelling first, then with
    diacritics folded to catch transcription differences.
    """
    start_date_parsed = h.extract_date(context.dataset, start_date)
    # extract_date falls back to the original text when it can't parse a date,
    # so a blank cell yields [""] rather than []. Without a usable start date we
    # can't judge recency, so skip the check rather than warn spuriously.
    if len(start_date_parsed) == 0 or not start_date_parsed[0]:
        return
    if CHECK_CONSOLIDATED_DATE < start_date_parsed[0]:
        # Don't bother checking recent entries since the consolidated text
        # may not have been updated yet.
        return

    act = get_consolidated_act(context, source_url)
    if act is None:
        return
    consolidated_celex = act.latest_consolidated
    assert consolidated_celex is not None
    consolidated_act_text = _law_normalized(context, act)
    if consolidated_act_text is None:
        return
    for name in names:
        name = context.lookup_value("garbage_collect_original_name", name, default=name)
        norm_name = normalize(name)
        if norm_name is not None and norm_name in consolidated_act_text:
            continue

        # Not found without asciifying — try again with diacritics stripped.
        ascii_name = normalize(name, ascii=True)
        ascii_law = _law_ascii(context, act)
        if ascii_name and ascii_law and ascii_name in ascii_law:
            context.log.info(
                "Name found in consolidated text only after asciifying",
                name=name,
                ascii_name=ascii_name,
                list_id=list_id,
                source_url=source_url,
                consolidated_celex=consolidated_celex,
            )
        else:
            # A newer consolidation than the one we cached may list the name.
            expression_request = cellar.expression_request(consolidated_celex)
            context.clear_url(
                expression_request.url,
                method=expression_request.method,
                data=expression_request.data,
            )
            context.clear_url(
                act.request.url, method=act.request.method, data=act.request.data
            )
            context.log.warning(
                "Name not found in consolidated regulation text",
                name=name,
                ascii_name=ascii_name,
                list_id=list_id,
                source_url=source_url,
                consolidated_celex=consolidated_celex,
                start_date=start_date,
            )


def check_canonical_feeds(
    context: Context,
    linker: Linker[Entity],
    entity_id: str,
    row_idx: int,
    list_id: str,
    name: str,
    entity_type: str,
    country: str,
) -> bool:
    """Report every canonical EU feed a journal row has already graduated into.

    Use this to find rows that can be retired from the sheet. Returns True when the
    designation has reached a feed that supersedes this dataset, so all referents are
    inspected rather than stopping at the first one that happens to match.
    """
    canonical_id = linker.get_canonical(entity_id)
    found: dict[str, str] = {}
    for other_id in linker.get_referents(canonical_id):
        for prefix in CANONICAL_FEEDS:
            if other_id.startswith(prefix):
                found.setdefault(prefix, other_id)
    for prefix, other_id in sorted(found.items()):
        context.log.warning(
            f"Row {row_idx} is also present in {CANONICAL_FEEDS[prefix]}: {other_id}",
            row_idx=row_idx,
            list_id=list_id,
            other_id=other_id,
            name=name,
            entity_type=entity_type,
            country=country,
        )
    return not RETIRE_FEEDS.isdisjoint(found)


def report_gc_range(context: Context, first_row: int, last_row: int) -> None:
    """Report a contiguous run of sheet rows that can be deleted."""
    context.log.warning(
        f"Rows {first_row}:{last_row} are in other datasets",
        first_row=first_row,
        last_row=last_row,
        names=[
            GC_ROWS[idx] for idx in range(first_row, last_row + 1) if idx in GC_ROWS
        ],
    )


def crawl_unconsolidated_row(
    context: Context, linker: Linker[Entity], row_idx: int, row: dict[str, str]
) -> None:
    """Emit an entity from a journal row not covered by the main EU XML feed.

    Use this for the current journal spreadsheet, where rows should eventually
    disappear once their entities are available in the canonical EU sources.
    """
    list_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()
    reg_number = row.pop("registrationNumber").strip()
    source_url = row.pop("Source URL").strip()
    program_code = extract_program_code(context, source_url)

    context.log.debug(f"Processing row #{row_idx}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(list_id, name, country)
    if entity.id is None:
        context.log.warning(
            f"Could not generate unique ID for row {row_idx}: {name}",
            row_idx=row_idx,
            list_id=list_id,
            name=name,
            entity_type=entity_type,
            country=country,
        )
        GC_ROWS[row_idx] = name
        return
    context.log.debug(f"Unique ID {entity.id}")

    start_date = row.pop("startDate")
    names = h.multi_split(name, ";")
    check_in_consolidated_act_text(context, start_date, names, list_id, source_url)

    if check_canonical_feeds(
        context, linker, entity.id, row_idx, list_id, name, entity_type, country
    ):
        GC_ROWS[row_idx] = name

    dob = row.pop("DOB")
    if entity.schema.is_a("Organization"):
        h.apply_dates(entity, "incorporationDate", h.multi_split(dob, ";"))
    elif entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", h.multi_split(dob, ";"))
    entity.add("birthPlace", row.pop("POB"), quiet=True)
    entity.add("country", h.multi_split(country, ";"))
    entity.add("name", names)
    entity.add("previousName", h.multi_split(row.pop("previousName"), ";"))
    entity.add("alias", h.multi_split(row.pop("Alias"), ";"))
    entity.add("weakAlias", h.multi_split(row.pop("weakAlias"), ";"))
    entity.add_cast("Person", "passportNumber", h.multi_split(row.pop("passport"), ";"))
    entity.add("taxNumber", h.multi_split(row.pop("taxNumber"), ";"), quiet=True)
    entity.add("kppCode", h.multi_split(row.pop("kppCode"), ";"), quiet=True)
    entity.add("idNumber", h.multi_split(row.pop("idNumber"), ";"), quiet=True)
    entity.add("imoNumber", row.pop("imoNumber"), quiet=True)
    entity.add("notes", row.pop("Notes").strip())
    entity.add("position", h.multi_split(row.pop("Position", None), ";"), quiet=True)
    entity.add("address", h.multi_split(row.pop("Address", None), ";"), quiet=True)
    entity.add("email", h.multi_split(row.pop("email"), ";"), quiet=True)
    entity.add("website", h.multi_split(row.pop("website"), ";"), quiet=True)
    entity.add("gender", row.pop("Gender", None), quiet=True)
    entity.add("sourceUrl", h.multi_split(source_url, ";"))
    for reg_num in h.multi_split(reg_number, ";"):
        if "ru" in entity.get("country") and OGRN.is_valid(reg_num):
            entity.add("ogrnCode", reg_num)
        else:
            entity.add("registrationNumber", reg_num)

    for related_name in h.multi_split(row.pop("related"), ";"):
        related = context.make("LegalEntity")
        related.id = context.make_id(related_name, entity.id)
        related.add("name", related_name)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(related.id, entity.id)
        rel.add("subject", related)
        rel.add("object", entity)

        context.emit(related)
        context.emit(rel)

    sanction = h.make_sanction(
        context,
        entity,
        key=program_code,
        program_key=h.lookup_sanction_program_key(context, program_code),
    )
    h.apply_date(sanction, "startDate", start_date)
    entity.add("topics", "sanction")

    for public_key in h.multi_split(row.pop("crypto wallet"), [";"]):
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(public_key)
        wallet.add("publicKey", public_key)
        wallet.add("holder", entity)
        wallet.add("topics", "sanction")

        wallet_sanction = h.make_sanction(
            context,
            wallet,
            key=program_code,
            program_key=h.lookup_sanction_program_key(context, program_code),
        )
        h.apply_date(wallet_sanction, "startDate", start_date)

        context.emit(wallet)
        context.emit(wallet_sanction)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl_context_row(context: Context, row_idx: int, row: dict[str, str]) -> None:
    """Emit a context-only entity for rows already covered by canonical EU feeds."""
    list_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()
    reg_number = row.pop("registrationNumber").strip()
    source_url = row.pop("Source URL").strip()

    context.log.debug(f"Processing row #{row_idx}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(list_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")

    dob = row.pop("DOB")
    if entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", h.multi_split(dob, ";"))
    entity.add("birthPlace", row.pop("POB"), quiet=True)
    # entity.add("country", h.multi_split(country, ";"))
    entity.add("name", h.multi_split(name, ";"))
    # entity.add("previousName", h.multi_split(row.pop("previousName"), ";"))
    entity.add("alias", h.multi_split(row.pop("Alias"), ";"))
    entity.add("weakAlias", h.multi_split(row.pop("weakAlias"), ";"))
    entity.add_cast("Person", "passportNumber", h.multi_split(row.pop("passport"), ";"))
    entity.add("taxNumber", h.multi_split(row.pop("taxNumber"), ";"), quiet=True)
    entity.add("kppCode", h.multi_split(row.pop("kppCode"), ";"), quiet=True)
    entity.add("idNumber", h.multi_split(row.pop("idNumber"), ";"), quiet=True)
    entity.add("imoNumber", row.pop("imoNumber"), quiet=True)
    # entity.add("notes", row.pop("Notes").strip())
    # entity.add("position", h.multi_split(row.pop("Position", None), ";"), quiet=True)
    # entity.add("address", h.multi_split(row.pop("Address", None), ";"), quiet=True)
    entity.add("email", h.multi_split(row.pop("email"), ";"), quiet=True)
    entity.add("website", h.multi_split(row.pop("website"), ";"), quiet=True)
    entity.add("gender", row.pop("Gender", None), quiet=True)
    entity.add("sourceUrl", h.multi_split(source_url, ";"))
    if "ru" in entity.get("country"):
        entity.add("ogrnCode", h.multi_split(reg_number, ";"))
    else:
        entity.add("registrationNumber", h.multi_split(reg_number, ";"))

    context.emit(entity)
    context.audit_data(
        row,
        ignore=[
            "related",
            "startDate",
            "Address",
            "Notes",
            "previousName",
            "Position",
            "crypto wallet",
        ],
    )


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


def has_overlong_name(names: h.Names) -> bool:
    """Whether any value is longer than the FtM name type can carry.

    The character rules cannot see this shape: a comma-joined a.k.a. run has
    nothing irregular in it but the number of names it holds, so without this
    the longest of them are the ones that never reach a reviewer.
    """
    for _prop, values in names.as_langtexts():
        for value in values:
            if len(value.text) > NAME_MAX_LENGTH:
                return True
    return False


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
            is_irregular = is_irregular or has_overlong_name(names)
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

            context.emit(entity, external=True)
            context.emit(sanction, external=True)


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
    # Current journal rows that are not yet present in the canonical EU feeds.
    path = context.fetch_resource("unconsolidated.csv", context.data_url)
    linker = get_dataset_linker(context.dataset)
    with open(path) as infh:
        for idx, row in enumerate(csv.DictReader(infh)):
            crawl_unconsolidated_row(context, linker, idx + 2, row)

    # Historical rows retained for context and link checks.
    context_url = context.data_url.replace("gid=0", "gid=1314630186")
    assert context_url != context.data_url
    path = context.fetch_resource("context.csv", context_url)
    with open(path) as infh:
        for idx, row in enumerate(csv.DictReader(infh)):
            crawl_context_row(context, idx + 2, row)

    # Collapse retirable rows into contiguous runs so they can be deleted in one go.
    seq_start = 0
    seq_max = 0
    for row_idx in sorted(GC_ROWS):
        if row_idx != seq_max + 1:
            if seq_start != 0:
                report_gc_range(context, seq_start, seq_max)
            seq_start = row_idx
        seq_max = row_idx

    if seq_start != 0:
        report_gc_range(context, seq_start, seq_max)

    crawl_csv_consolidated(context)
    crawl_csv_amendments(context)

    # Discovery emits no entity and costs minutes of CELLAR queries, so
    # `zavod --debug crawl` skips it and iterating on the CSVs stays fast.
    if settings.DEBUG is False:
        check_new_amendments(context)
        check_consolidation_pins(context)

    # Warn rather than raise: the dataset keeps publishing the source wording
    # while the name review backlog is worked through.
    assert_all_accepted(context, raise_on_unaccepted=False)
