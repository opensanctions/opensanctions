import csv
import re
from functools import cache
from datetime import timedelta, datetime

from normality import normalize, squash_spaces
from nomenklatura.resolver import Linker
from rigour.ids.ogrn import OGRN

from zavod.extract.zyte_api import fetch_html
from zavod.integration import get_dataset_linker
from zavod import Context, Entity, helpers as h

# Some entities come from the full text of the consolidated COUNCIL REGULATION (EU) No 833/2014.
#  This consolidated document is treated differently from standard EUR-Lex lookups.
SPECIAL_CASE_URL = (
    "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:02014R0833-20240625"
)
# year/number or number/year with optional suffix
FIRST_CODE_RE = re.compile(
    r"\b(?:No\s+)?(\d{1,4}/\d{1,4})(?:/[A-Z]{2,5})?\b", re.IGNORECASE
)
# Yesterday 2026-03-05, https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32024D1484
# still showed https://eur-lex.europa.eu/legal-content/EN/AUTO/?uri=CELEX:02024D1484-20251120
# as latest instead of https://eur-lex.europa.eu/legal-content/EN/AUTO/?uri=CELEX:02024D1484-20251222
# (See timestamp at the end of each URL)
CHECK_CONSOLIDATED_DATE = h.backdate(datetime.now(), timedelta(days=90))

GC_ROWS = []


@cache
def extract_program_code(context, source_url):
    """Fetch EU act code from a EUR-Lex page."""
    if SPECIAL_CASE_URL in source_url:
        return "833/2014"
    program_xpath = "//div[@class='eli-main-title']/p[@class='oj-doc-ti']/text()"
    doc = fetch_html(context, source_url, program_xpath, cache_days=365)
    program_nodes = doc.xpath(program_xpath)
    if not program_nodes:
        context.log.warning(f"Could not find program for {source_url}")
        return
    title = squash_spaces(program_nodes[-1])  # always the last one
    # Extract the first EU act code (e.g., '2024/254') from a title.
    match = FIRST_CODE_RE.search(title)
    if not match:
        context.log.warning(
            f"No EU codes found in program name: {title}",
            source_url=source_url,
        )
        return
    return match.group(1)


# SearchResult


def wait_for_xpath_actions(xpath: str) -> list[dict[str, str | int]]:
    return [
        {
            "action": "waitForNavigation",
            "waitUntil": "networkidle0",
            "timeout": 31,
            "onError": "return",
        },
        {
            "action": "waitForSelector",
            "selector": {
                "type": "xpath",
                "value": xpath,
                "state": "visible",
            },
            "timeout": 15,
            "onError": "return",
        },
    ]


def get_consolidated_url(context: Context, source_url: str) -> str | None:
    """Given a EUR-Lex source URL for an amendment, return the URL of its consolidated version."""
    eurlex_actions = [
        {
            "action": "waitForSelector",
            "selector": {"type": "css", "value": ".EurlexContent"},
        }
    ]
    eurlex_validator = './/div[@class="EurlexContent"]'

    # Step 1: find what this amendment modifies
    all_url = source_url.replace("/TXT/", "/ALL/")
    doc = fetch_html(
        context,
        all_url,
        eurlex_validator,
        cache_days=1,
        actions=eurlex_actions,
        absolute_links=True,
    )
    original_celex: str | None = None
    for table in doc.xpath(".//table[@id='relatedDocsTbMS']"):
        # The <th> header cells embed <select> filter widgets whose text corrupts
        # parse_html_table's slugified keys (e.g. "relation_all_modifies" instead
        # of "relation").  Strip them before parsing.
        for select in table.xpath(".//thead//th/select"):
            select.getparent().remove(select)
        rows = [h.cells_to_str(row) for row in h.parse_html_table(table)]
        act_values = {r.get("act") for r in rows if r.get("act")}
        assert len(act_values) <= 1, (
            f"Multiple CELEX numbers in amendments table for {source_url}: {act_values}"
        )
        for row_strs in rows:
            if row_strs.get("relation") in ("Modifies", "Extended validity"):
                original_celex = row_strs.get("act")
                break
        if original_celex:
            break

    if not original_celex:
        context.log.warning(
            "Could not find original act in amendment relations table",
            source_url=source_url,
        )
        return None

    # Step 2: fetch the original act page and find the latest consolidated version
    # from the #consLegVersions nav. The current/latest entry has class "current active".
    original_url = (
        f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{original_celex}"
    )
    orig_doc = fetch_html(
        context,
        original_url,
        eurlex_validator,
        cache_days=1,
        actions=eurlex_actions,
        absolute_links=True,
    )
    for link in orig_doc.xpath(".//div[@id='consLegVersions']//a"):
        # Just take the first one since they seem to be ordered by descending date
        # and they don't always have the 'active' class.
        href = link.get("href")
        if href:
            return href

    context.log.warning(
        "Could not find consolidated version link on original act page",
        source_url=source_url,
        original_celex=original_celex,
    )
    return None


def get_consolidated_text(context: Context, consolidated_url: str) -> str | None:
    """Fetch and return the full text of a EUR-Lex consolidated regulation."""
    regulation_xpath = ".//div[@id='PP4Contents']"
    doc = fetch_html(
        context,
        consolidated_url,
        regulation_xpath,
        actions=wait_for_xpath_actions(regulation_xpath),
        cache_days=1,
    )
    regulation_div = doc.xpath(regulation_xpath)
    if not regulation_div:
        context.log.warning("Could not extract regulation text", url=consolidated_url)
        return None
    return squash_spaces(regulation_div[0].text_content())


@cache
def _law_normalized(context: Context, consolidated_url: str) -> str | None:
    text = get_consolidated_text(context, consolidated_url)
    return normalize(text) if text is not None else None


@cache
def _law_ascii(context: Context, consolidated_url: str) -> str | None:
    text = get_consolidated_text(context, consolidated_url)
    return normalize(text, ascii=True) if text is not None else None


def check_in_consolidated_act_text(
    context: Context, start_date: str, names: list[str], row_id: str, source_url: str
) -> None:
    """Warn if any name in `names` is absent from the consolidated regulation text.

    Looks up the consolidated version of whatever regulation `source_url` amends,
    so the check works for any EU sanctions regime, not just the Russia regulations.
    Two rounds: first without asciifying (preserves non-Latin scripts), then with
    asciifying (folds visually similar diacritics like Ş/Ș). Logs at info level
    if only the ascii round finds the name, so the source data can be corrected.
    """
    start_date_parsed = h.extract_date(context.dataset, start_date)
    if len(start_date_parsed) == 0 or CHECK_CONSOLIDATED_DATE < start_date_parsed[0]:
        # Don't bother checking recent entries since the consolidated text
        # may not have been updated yet.
        return

    consolidated_url = get_consolidated_url(context, source_url)
    if consolidated_url is None:
        return
    consolidated_act_text = _law_normalized(context, consolidated_url)
    if consolidated_act_text is None:
        return
    for name in names:
        name = context.lookup_value("garbage_collect_original_name", name, default=name)
        norm_name = normalize(name)
        if norm_name is not None and norm_name in consolidated_act_text:
            continue

        # Not found without asciifying — try again with diacritics stripped.
        ascii_name = normalize(name, ascii=True)
        ascii_law = _law_ascii(context, consolidated_url)
        if ascii_name and ascii_law and ascii_name in ascii_law:
            context.log.info(
                "Name found in consolidated text only after asciifying",
                name=name,
                ascii_name=ascii_name,
                row_id=row_id,
                source_url=source_url,
                consolidated_url=consolidated_url,
            )
        else:
            context.log.warning(
                "Name not found in consolidated regulation text",
                name=name,
                ascii_name=ascii_name,
                row_id=row_id,
                source_url=source_url,
                consolidated_url=consolidated_url,
                start_date=start_date,
            )


def crawl_unconsolidated_row(
    context: Context, linker: Linker[Entity], row_idx: int, row: dict[str, str]
) -> None:
    """Process one row of the CSV data

    Unconsolidated between EU Journal and XML, not in the consolidated legislation sense.
    """
    row_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()
    reg_number = row.pop("registrationNumber").strip()
    source_url = row.pop("Source URL").strip()
    program_code = extract_program_code(context, source_url)

    context.log.debug(f"Processing row #{row_idx}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(row_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")

    start_date = row.pop("startDate")
    names = h.multi_split(name, ";")
    check_in_consolidated_act_text(context, start_date, names, row_id, source_url)

    canonical_id = linker.get_canonical(entity.id)
    for other_id in linker.get_referents(canonical_id):
        if other_id.startswith("eu-fsf-"):
            context.log.warning(
                f"Row {row_idx} is also present in FSF XML: {other_id}",
                row_id=row_id,
                other_id=other_id,
                name=name,
                entity_type=entity_type,
                country=country,
            )
            GC_ROWS.append(row_idx)
            break
        if other_id.startswith("eu-sancmap-"):
            context.log.warning(
                f"Row {row_idx} is also present in EU Sanctions map: {other_id}",
                row_id=row_id,
                other_id=other_id,
                name=name,
                entity_type=entity_type,
                country=country,
            )
            GC_ROWS.append(row_idx)
            break
        if other_id.startswith("eu-tb-"):
            context.log.warning(
                f"Row {row_idx} is also present in EU Travel Bans: {other_id}",
                row_id=row_id,
                other_id=other_id,
                name=name,
                entity_type=entity_type,
                country=country,
            )
            break

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
    entity.add_cast("Person", "passportNumber", h.multi_split(row.pop("passport"), ";"))
    entity.add("taxNumber", h.multi_split(row.pop("taxNumber"), ";"), quiet=True)
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
    """Process one row of the contextual CSV data"""
    row_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()
    reg_number = row.pop("registrationNumber").strip()
    source_url = row.pop("Source URL").strip()

    context.log.debug(f"Processing row #{row_idx}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(row_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")

    dob = row.pop("DOB")
    if entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", h.multi_split(dob, ";"))
    entity.add("birthPlace", row.pop("POB"), quiet=True)
    # entity.add("country", h.multi_split(country, ";"))
    entity.add("name", h.multi_split(name, ";"))
    # entity.add("previousName", h.multi_split(row.pop("previousName"), ";"))
    entity.add("alias", h.multi_split(row.pop("Alias"), ";"))
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


def crawl(context: Context):
    # Round 1: unconsolidated.csv with the latest journal updates
    # Unconsolidated between EU Journal and XML, not in the consolidated legislation sense.
    path = context.fetch_resource("unconsolidated.csv", context.data_url)
    linker = get_dataset_linker(context.dataset)
    with open(path, "rt") as infh:
        for idx, row in enumerate(csv.DictReader(infh)):
            crawl_unconsolidated_row(context, linker, idx + 2, row)

    # Round 2: context.csv with older entries now present in main databases
    context_url = context.data_url.replace("gid=0", "gid=1314630186")
    assert context_url != context.data_url
    path = context.fetch_resource("context.csv", context_url)
    with open(path, "rt") as infh:
        for idx, row in enumerate(csv.DictReader(infh)):
            crawl_context_row(context, idx + 2, row)

    # Warn about rows that are also in other datasets
    seq_start = 0
    seq_max = 0
    for row_idx in sorted(set(GC_ROWS)):
        if row_idx != seq_max + 1:
            if seq_start != 0:
                context.log.warn(f"Row {seq_start}:{seq_max} is in other datasets")
            seq_start = row_idx
        seq_max = row_idx

    if seq_start != 0:
        context.log.warn(f"Row {seq_start}:{seq_max} is in other datasets")
