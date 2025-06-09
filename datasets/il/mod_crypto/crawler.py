import csv
from typing import Dict
from pathlib import Path

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html

ID_FIELDS = [("id_no", "id_country"), ("residency_no", "residency_country")]
LOCAL_PATH = Path(__file__).parent


def remove_zero_width_space(row):
    return {
        k: (v.replace("\u200b", "") if isinstance(v, str) else v)
        for k, v in row.items()
    }


def write_csv_for_manual_diff(context, container):
    tables = container.xpath('//table[@class="ms-rteTable-4"]')
    if len(tables) != 2:
        context.log.warning(f"Expected 2 tables, found {len(tables)}")

    output_paths = [
        LOCAL_PATH / "releases.csv",
        LOCAL_PATH / "wallets.csv",
    ]

    for table, path in zip(tables, output_paths):
        rows = [
            h.cells_to_str(row)
            for row in h.parse_html_table(table, ignore_colspan={"7"})
        ]
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


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
        h.apply_date(person, "birthDate", row.pop("dob"))
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
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(account_id)
        wallet.set("publicKey", account_id)
        platform = row.pop("platform")
        if platform:
            wallet.set("mangingExchange", platform)
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
    doc = fetch_html(context, context.dataset.url, content_xpath, cache_days=1)
    container = doc.xpath(content_xpath)[0]
    # Write a CSV snapshot to check the diff manually (git diff).
    # Review for any new releases or persons/wallets added.
    # The key things to check are
    # - the table of releases - are there any new ones?
    # - The table of persons/wallets - does it look like anything's been added there?
    # If updated, reflect changes in the Google Sheet and commit the new CSV:
    # git add -f datasets/il/mod_crypto/releases.csv
    # git add -f datasets/il/mod_crypto/wallets.csv
    write_csv_for_manual_diff(context, doc)
    h.assert_dom_hash(container, "203b99615f06e11bf4af3273e2cb46506c0804c4")

    # At the time of writing, the table on the web page is missing some public keys,
    # so we maintain the data manually in a google sheet, but dump the table to csv
    # to be able to see what changed quickly.
    src = context.fetch_resource("source.csv", context.data_url)
    with open(src, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = remove_zero_width_space(row)
            crawl_csv_row(context, row)
