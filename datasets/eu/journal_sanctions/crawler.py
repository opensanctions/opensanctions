import csv
import re
from datetime import timedelta
from functools import cache

from lxml import html
from nomenklatura.resolver import Linker
from normality import normalize
from rigour.ids.ogrn import OGRN
from zavod.integration import get_dataset_linker
from zavod.shed.ojeu import cellar
from zavod.shed.ojeu.celex import normalize as normalize_celex

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
