import csv
from pathlib import Path
from typing import Dict

from normality import squash_spaces
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

ID_FIELDS = [("id_no", "id_country"), ("residency_no", "residency_country")]
LOCAL_PATH = Path(__file__).parent


def remove_zero_width_space(row):
    return {
        k: (v.replace("\u200b", "") if isinstance(v, str) else v)
        for k, v in row.items()
    }


def normalize_address(addr):
    return "".join(HOMOGLYPHS.get(c, c) for c in addr)


def write_csv_for_manual_diff(table, path):
    with open(path, "w") as f:
        writer = csv.writer(f)
        for row in table.findall(".//tr"):
            cells = [
                squash_spaces(c.text_content())
                for c in row.xpath(".//*[self::td or self::th]")
            ]
            writer.writerow(cells)


def crawl_csv_row(context: Context, row: Dict[str, str]):
    person = None
    entity = None
    wallets = []

    # --- Person ---
    schema = row.pop("schema")
    name = row.pop("name", None)
    if schema == "Person" and (row.get("id_no") or row.get("passport_no") or name):
        person = context.make("Person")
        person.id = context.make_id(row.get("id_no") or row.get("passport_no") or name)
        h.apply_name(person, full=name, lang="eng")
        h.apply_date(person, "birthDate", squash_spaces(row.pop("dob")))
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
    account_id = row.pop("account/wallet_id")
    if account_id:
        account_id = normalize_address(account_id)
        if not is_latin(account_id):
            context.log.warning(f"Non-latin account ID: {account_id}")
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(account_id)
        wallet.set("publicKey", account_id)
        platform = row.pop("platform")
        if platform:
            wallet.set("managingExchange", platform)
        currency = row.pop("currency")
        if currency:
            wallet.set("currency", currency)
        if person or entity:
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


def crawl(context: Context):
    # Get a warning when a notice has been issued
    content_xpath = ".//main"
    doc = fetch_html(context, context.dataset.model.url, content_xpath, cache_days=1)
    container = doc.xpath(content_xpath)[0]
    # Write a CSV snapshot to check the diff manually (git diff).
    # Review for any new releases or persons/wallets added.
    # The key things to check are
    # - the table of releases - are there any new ones?
    # - The table of persons/wallets - does it look like anything's been added there?
    # If updated, reflect changes in the Google Sheet and commit the new CSV:
    # git add -f datasets/il/mod_crypto/releases.csv
    # git add -f datasets/il/mod_crypto/wallets.csv
    tables = container.xpath('//table[@class="ms-rteTable-4"]')
    if len(tables) != 2:
        context.log.warning(f"Expected 2 tables, found {len(tables)}")

    write_csv_for_manual_diff(tables[0], LOCAL_PATH / "releases.csv")
    write_csv_for_manual_diff(tables[1], LOCAL_PATH / "wallets.csv")
    h.assert_dom_hash(container, "36f0215ed4ac0e1e8b76df5bf565f72aaac5eaba")

    # At the time of writing, the table on the web page is missing some public keys,
    # so we maintain the data manually in a google sheet, but dump the table to csv
    # to be able to see what changed quickly.
    src = context.fetch_resource("source.csv", context.data_url)
    with open(src, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = remove_zero_width_space(row)
            crawl_csv_row(context, row)
