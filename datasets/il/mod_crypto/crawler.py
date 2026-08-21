import csv
import re
import shutil
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast
from urllib.parse import urljoin
from lxml.html import HtmlElement

from normality import squash_spaces
from rigour.mime.types import CSV
from rigour.text.scripts import is_latin
from zavod.extract.zyte_api import fetch_html

from zavod import Context
from zavod import helpers as h

HOMOGLYPHS = {
    "ᴄ": "c",
    "ᴑ": "o",
    "ᴠ": "v",
    "ᴡ": "w",
    "ᴢ": "z",
    "Α": "A",
    "Β": "B",
    "Ε": "E",
    "Ζ": "Z",
    "Η": "H",
    "ϳ": "j",
    "Κ": "K",
    "Μ": "M",
    "Ν": "N",
    "ο": "o",
    "Ρ": "P",
    "Ϲ": "C",
    "Τ": "T",
    "Υ": "Y",
    "Χ": "X",
    "а": "a",
    "А": "A",
    "В": "B",
    "ԁ": "d",
    "е": "e",
    "Е": "E",
    "ѕ": "s",
    "Ѕ": "S",
    "ј": "j",
    "Ј": "J",
    "ԛ": "q",
    "М": "M",
    "Н": "H",
    "о": "o",
    "р": "p",
    "Р": "P",
    "с": "c",
    "С": "C",
    "Ԍ": "G",
    "Т": "T",
    "Ү": "Y",
    "х": "x",
    "Х": "X",
    "ԝ": "w",
    "Ԝ": "W",
    "հ": "h",
    "ո": "n",
    "ս": "u",
    "Ս": "U",
    "օ": "o",
}

ZERO_WIDTH_SPACE = "\u200b"
ID_FIELDS = [("id_no", "id_country"), ("residency_no", "residency_country")]
LOCAL_PATH = Path(__file__).parent
SOURCE_FILE = "seizures.csv"
CONTENT_XPATH = ".//main"
TABLES_XPATH = '//table[@class="ms-rteTable-4"]'
# Values in the wallet table are compared against seizures.csv without assuming
# which column holds what: the table's columns are titled name, ID, passport,
# date of birth, wallet/account identifier and currency, but blocks disagree -
# ASO 1/25 lists phone numbers under the identifier heading. So every cell is
# reduced to the identifier-like tokens it contains (a wallet address, an
# account, passport or phone number: alphanumeric, at least 8 characters, and
# containing a digit) and looked up against every value the CSV holds for that
# order. Names are deliberately not compared: they vary in case and spelling,
# and a person named on the page carries an identifier in practice.
TOKEN = re.compile(r"^(?=[A-Za-z0-9]*\d)[A-Za-z0-9]{8,}$")
# Annotations the page appends to an address, e.g. "(Address tag 1051035076) *".
ANNOTATION = re.compile(r"\(.*?\)")
DATE_VALUE = re.compile(r"^\d{1,4}[./-]\d{1,2}[./-]\d{2,4}$")
# Floors for the monitor's own reading of the page. Well below the current 35
# orders and ~1080 tokens, but high enough to catch a page whose markup changed
# under us and now parses as almost empty - which would otherwise read as
# "nothing new to review".
MIN_PAGE_ORDERS = 25
MIN_PAGE_TOKENS = 700
# Rows of the releases table, and order blocks of the wallet table, are labelled
# with the Hebrew abbreviation for a seizure order (צו תפיסה) and its number.
ORDER_MARKER = re.compile(r"^צ[.'\"׳]?\s*ת\b")
ORDER_NUMBER = re.compile(r"(\d{1,3})\s*/\s*(\d{2})\b")


@dataclass(frozen=True)
class PageOrder:
    """A seizure order as listed in the releases table of the source page."""

    number: str
    label: str
    last_updated: str
    validity: str
    documents: tuple[str, ...]


def remove_zero_width_space(row: dict[str, str]) -> dict[str, str]:
    return {
        k: (v.replace(ZERO_WIDTH_SPACE, "") if isinstance(v, str) else v)
        for k, v in row.items()
    }


def normalize_address(addr: str) -> str:
    return "".join(HOMOGLYPHS.get(c) or c for c in addr)


def write_csv_for_manual_diff(table: HtmlElement, path: Path) -> None:
    with open(path, "w") as f:
        writer = csv.writer(f)
        for row in table.findall(".//tr"):
            cells = [
                squash_spaces(cast(HtmlElement, c).text_content())
                for c in h.xpath_elements(row, ".//*[self::td or self::th]")
            ]
            writer.writerow(cells)


def order_number(text: str) -> str | None:
    """The canonical `n/yy` key of a seizure order named in a label or CSV cell.

    The page writes an order as `צ.ת 18/26` and `seizures.csv` as `ASO 18/26` or
    `ASO - 34/24`, so both sides are reduced to the number alone.
    """
    match = ORDER_NUMBER.search(text)
    if match is None:
        return None
    return f"{int(match.group(1))}/{match.group(2)}"


def row_cells(row: HtmlElement) -> list[str]:
    return [
        squash_spaces(cast(HtmlElement, cell).text_content()).replace(
            ZERO_WIDTH_SPACE, ""
        )
        for cell in h.xpath_elements(row, ".//*[self::td or self::th]")
    ]


def parse_releases(table: HtmlElement, base_url: str) -> dict[str, PageOrder]:
    """Read the releases table as one entry per seizure order.

    An order is listed as its own row, sometimes followed by rows for its annex
    or an amendment. Those name the same order, so their documents are collected
    onto it rather than counted as further orders.
    """
    orders: dict[str, PageOrder] = {}
    for row in h.xpath_elements(table, ".//tr"):
        cells = row_cells(row)
        if len(cells) < 3:
            continue
        label = cells[0]
        number = order_number(label)
        if number is None:
            continue
        documents = tuple(
            urljoin(base_url, cast(str, anchor.get("href")))
            for anchor in h.xpath_elements(row, ".//a[@href]")
        )
        existing = orders.get(number)
        if existing is None:
            orders[number] = PageOrder(number, label, cells[1], cells[2], documents)
            continue
        # Prefer the order's own row for the dates; an annex row only adds a link.
        if ORDER_MARKER.match(existing.label) or not ORDER_MARKER.match(label):
            orders[number] = replace(existing, documents=existing.documents + documents)
        else:
            orders[number] = PageOrder(
                number, label, cells[1], cells[2], existing.documents + documents
            )
    return orders


def value_tokens(value: str) -> set[str]:
    """The identifier-like tokens a table cell or CSV cell states.

    A cell can hold several, comma-separated, and an address can carry a
    parenthesised annotation and a trailing asterisk.
    """
    text = ANNOTATION.sub(" ", value).replace("*", " ")
    tokens = set()
    for part in re.split(r"[,;\n]", text):
        part = part.strip()
        if len(part) == 0 or DATE_VALUE.match(part):
            continue
        if TOKEN.match(part):
            tokens.add(normalize_address(part))
    return tokens


def parse_wallet_tokens(table: HtmlElement) -> dict[str, set[str]]:
    """Read the wallet table as the identifiers listed under each seizure order.

    The table has no order column: each order is introduced by a row holding only
    its number, and the rows below it belong to that order until the next one.
    """
    wallets: dict[str, set[str]] = {}
    number: str | None = None
    for row in h.xpath_elements(table, ".//tr"):
        cells = row_cells(row)
        if ORDER_MARKER.match(cells[0] if len(cells) > 0 else ""):
            number = order_number(cells[0])
            if number is None:
                continue
            wallets.setdefault(number, set())
            # A marker row can carry the order's first wallet in its later cells.
            cells = cells[1:]
        if number is None:
            continue
        for cell in cells:
            wallets[number].update(value_tokens(cell))
    return wallets


def csv_tokens(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    """Every identifier-like value the CSV holds, per seizure order."""
    known: dict[str, set[str]] = {}
    for row in rows:
        number = order_number(row["order_id"])
        if number is None:
            continue
        tokens = known.setdefault(number, set())
        for value in row.values():
            tokens.update(value_tokens(value))
    return known


def check_source_page(context: Context, rows: list[dict[str, str]]) -> None:
    """Warn about anything on the source page that seizures.csv does not cover.

    The data itself is maintained in seizures.csv, so this only decides what a
    maintainer is asked to look at: a seizure order the CSV doesn't know, or an
    order whose page listing has grown wallets the CSV is missing. The CSV holds
    more than the page - it is transcribed from the order PDFs, which name
    wallets the page omits - so only page-not-in-CSV is reported.
    """
    assert context.dataset.url
    doc = fetch_html(context, context.dataset.url, CONTENT_XPATH, cache_days=1)
    container = h.xpath_element(doc, CONTENT_XPATH)
    tables = h.xpath_elements(container, TABLES_XPATH, expect_exactly=2)
    # The snapshots serve two readers: committed next to the crawler they give a
    # reviewer a git diff of what the page said when it was last looked at, and
    # exported as resources they let whoever handles a warning read the full page
    # table from the archived run, without needing to unblock the site again.
    for table, name, title in (
        (tables[0], "page_releases.csv", "Seizure orders listed on the source page"),
        (tables[1], "page_wallets.csv", "Wallets listed on the source page"),
    ):
        write_csv_for_manual_diff(table, LOCAL_PATH / name)
        resource_path = context.get_resource_path(name)
        shutil.copy(LOCAL_PATH / name, resource_path)
        context.export_resource(resource_path, CSV, title)

    page_orders = parse_releases(tables[0], context.dataset.url)
    page_wallets = parse_wallet_tokens(tables[1])
    listed = sum(len(tokens) for tokens in page_wallets.values())
    if len(page_orders) < MIN_PAGE_ORDERS or listed < MIN_PAGE_TOKENS:
        context.log.warning(
            "Source page yielded fewer orders or values than expected",
            orders=len(page_orders),
            values=listed,
            url=context.dataset.url,
        )

    discovery = context.dataset.config.get("discovery", {})
    reviewed = {
        number
        for number in (
            order_number(str(order)) for order in discovery.get("reviewed_orders", [])
        )
        if number is not None
    }
    known = csv_tokens(rows)
    for number, order in sorted(page_orders.items()):
        if number in reviewed:
            continue
        if number not in known:
            context.log.warning(
                "Unreviewed seizure order",
                order=f"ASO {number}",
                label=order.label,
                last_updated=order.last_updated,
                validity=order.validity,
                documents=list(order.documents),
                # The values the page lists, so a first set of rows can be drafted
                # from the warning alone. The order PDF holds more.
                values=sorted(page_wallets.get(number, set())),
            )
            continue
        missing = sorted(page_wallets.get(number, set()) - known[number])
        if len(missing) > 0:
            context.log.warning(
                "Unreviewed values in a seizure order listing",
                order=f"ASO {number}",
                count=len(missing),
                values=missing,
                documents=list(order.documents),
            )
    for number in sorted(set(known) - set(page_orders) - reviewed):
        context.log.warning(
            "Seizure order is no longer listed on the source page",
            order=f"ASO {number}",
            url=context.dataset.url,
        )


def crawl_csv_row(context: Context, row: dict[str, str]) -> None:
    person = None
    entity = None
    country = None
    wallets = []

    # --- Person ---
    schema = row.pop("schema")
    name = row.pop("name", None)
    if schema == "Person" and (row.get("id_no") or row.get("passport_no") or name):
        person = context.make("Person")
        person.id = context.make_id(row.get("id_no") or row.get("passport_no") or name)
        h.apply_name(person, full=name, lang="eng")
        h.apply_date(
            person,
            "birthDate",
            squash_spaces(row.pop("dob")),
            two_digit_year_base=h.TWO_DIGIT_BIRTH_YEAR_BASE,
        )
        person.add("email", row.pop("email").split(";"))
        person.add("phone", row.pop("phone"))
        for alias in row.pop("alias").split(";"):
            h.apply_name(person, full=alias, alias=True)
        # Process identification documents (e.g., national ID, residency)
        for id_key, country_key in ID_FIELDS:
            id_number = row.pop(id_key)
            country = row.pop(country_key)
            if id_number:
                identification = h.make_identification(
                    context,
                    person,
                    id_number,
                    passport=False,
                    country=country,
                )
                # Emit an Identification entity if country is present
                if identification and country:
                    context.emit(identification)
        # Process passport
        if passport_number := row.pop("passport_no"):
            passport = h.make_identification(
                context,
                person,
                passport_number,
                passport=True,
                country=row.pop("passport_country"),
            )
            # Emit a Passport entity if country is present
            if passport and country:
                context.emit(passport)

        context.emit(person)

    # --- Legal Entity ---
    if schema == "LegalEntity":
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name)
        h.apply_name(entity, full=name, lang="eng")
        h.apply_name(entity, full=row.pop("alias"), alias=True)
        context.emit(entity)

    # --- Wallets --- are always created if wallet data is present
    # account_id = row.pop("account/wallet_id")
    wallet_address = row.pop("wallet_address")
    account_id = row.pop("account_id")
    # Use wallet_id if present, otherwise fall back to account_id
    # These are mutually exclusive in source data - we get either:
    # - wallet_address: On-chain address tied to a specific blockchain
    # - account_id: Platform account number tied to an exchange (e.g., Binance)
    identifier = wallet_address or account_id
    if identifier:
        identifier = normalize_address(identifier)
        if not is_latin(identifier):
            context.log.warning(f"Non-latin identifier: {identifier}")
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(identifier)
        wallet.set("publicKey", wallet_address)
        wallet.set("accountId", account_id)
        wallet.set("managingExchange", row.pop("platform"))
        wallet.set("currency", row.pop("currency"))
        wallet.set("holder", person or entity)
        wallets.append(wallet)

    # --- Sanction & Linking ---
    aso_id = row.pop("order_id")
    for wallet in wallets:
        sanction = h.make_sanction(context, wallet, key=aso_id)
        sanction.set("authorityId", aso_id)
        # Manually extracted from each order (pdf), it's the date it was issued
        h.apply_date(sanction, "startDate", row.pop("start_date"))
        # "Last Updated" column in the table of releases
        h.apply_date(sanction, "modifiedAt", row.pop("last_updated"))
        # "Validity of Issue" column in the table of releases
        h.apply_date(sanction, "endDate", row.pop("end_date"))
        if h.is_active(sanction):
            wallet.add("topics", "crime.terror")
        # "File Type" column in the table of releases
        # e.g., "​Seizure order (ASO 16/25) of the Minister of Defense"
        sanction.add("sourceUrl", row.pop("order_url"))
        # Links from the "Validity of Issue" column in the table of releases
        # e.g., "​Forfeiture Order (FO​ 18/24)"
        sanction.add("sourceUrl", row.pop("forfeiture_order_url"))
        # Links from the "File Type" column in the table of releases
        # e.g., "Annex of the Seizure Order (ASO - 56/23) - Wallet Details"
        sanction.add("sourceUrl", row.pop("annex_url"))
        context.emit(wallet)
        context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    # The dataset is maintained as seizures.csv next to the crawler (see
    # MAINTENANCE.md), transcribed from the seizure order PDFs because the web
    # page omits some public keys. The page itself is only monitored, so that a
    # new order or a newly listed wallet is reported for review.
    source_file = LOCAL_PATH / SOURCE_FILE
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        rows = [remove_zero_width_space(row) for row in csv.DictReader(fh)]
    for row in rows:
        crawl_csv_row(context, dict(row))

    try:
        check_source_page(context, rows)
    except Exception as exc:
        # The page is a monitor, not the source of the data: when it is blocked
        # or restructured, publish the CSV and report the check as an issue
        # rather than failing the whole run.
        context.log.warning("Source page check failed", error=str(exc))
