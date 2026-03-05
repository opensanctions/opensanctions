import csv
import re
from functools import cache, lru_cache
from typing import Dict
from normality import squash_spaces

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
CONSOLIDATED_LATEST = [
    "https://eur-lex.europa.eu/eli/reg/2014/833",
    "https://eur-lex.europa.eu/eli/dec/2014/512",
    "https://eur-lex.europa.eu/eli/reg/2024/2642",
]
# year/number or number/year with optional suffix
FIRST_CODE_RE = re.compile(
    r"\b(?:No\s+)?(\d{1,4}/\d{1,4})(?:/[A-Z]{2,5})?\b", re.IGNORECASE
)

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


def wait_for_xpath_actions(xpath: str) -> list[Dict[str, str | int]]:
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


@lru_cache(maxsize=1)
def get_regulation_text(context: Context):
    """Fetch and normalize the text content of the full regulations."""

    regulation_xpath = ".//div[@id='PP4Contents']"
    consolidated_search_xpath = (
        ".//div[@class='SearchResult']//a[contains(text(), 'Consolidated text')]"
    )
    unblock_validator = f"{regulation_xpath} | {consolidated_search_xpath}"
    unblock_actions = wait_for_xpath_actions(unblock_validator)

    all_texts = []
    for link in CONSOLIDATED_LATEST:
        doc = fetch_html(
            context,
            link,
            unblock_validator,
            actions=unblock_actions,
            absolute_links=True,
        )
        consolidated_search_results = doc.xpath(consolidated_search_xpath)
        if consolidated_search_results:
            if len(consolidated_search_results) > 1:
                context.log.warning(
                    "Multiple consolidated search results found, using the first one",
                    url=link,
                    count=len(consolidated_search_results),
                )
            context.log.info(
                "Found consolidated search result, following link", url=link
            )
            doc = fetch_html(
                context,
                consolidated_search_results[0].get("href"),
                regulation_xpath,
                actions=wait_for_xpath_actions(regulation_xpath),
            )
        regulation_div = doc.xpath(regulation_xpath)
        if not regulation_div:
            context.log.warning("Could not extract regulation text", url=link)
            continue  # skip this link and try the next
        regulation_text = regulation_div[0].text_content()
        all_texts.append(squash_spaces(regulation_text))
    return " ".join(all_texts)


def is_name_in_the_law(context, the_law: str, names: str, row_id: str, source_url: str):
    for name in h.multi_split(names, ";"):
        name = name.strip()
        if not name:
            continue
        if squash_spaces(name.lower()) not in the_law.lower():
            context.log.warn(
                "Name not found in consolidated regulation text",
                name=name,
                row_id=row_id,
                source_url=source_url,
            )


def crawl_unconsolidated_row(
    context: Context, linker: Linker[Entity], row_idx: int, row: Dict[str, str]
) -> None:
    """Process one row of the CSV data"""
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
    # Validate that the name is in the consolidated regulation text
    the_law = get_regulation_text(context)
    is_name_in_the_law(context, the_law, name, row_id, source_url)

    # Commented out since the 20 May journal updates added details like IDs and
    # cyrilic names which aren't in the XML yet but the entities are.
    # Uncomment when the details are added to FSF expected around 6 June
    # e.g. check whether https://www.opensanctions.org/statements/NK-VwonxcqhDhAzHKWXCdSdXd/?prop=registrationNumber
    # has 1674003000 from eu_fsf.
    #
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
    entity.add("name", h.multi_split(name, ";"))
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
    start_date = row.pop("startDate")
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


def crawl_context_row(context: Context, row_idx: int, row: Dict[str, str]) -> None:
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
