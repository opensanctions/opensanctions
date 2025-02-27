import csv
from typing import Dict

from normality.cleaning import collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_csv_row(context: Context, row: Dict[str, str]):
    # Create a Person entity if present in the data
    names = [
        name
        for name in [
            collapse_spaces(row.get("English Name")),
            collapse_spaces(row.get("Name Hebrew")),
            collapse_spaces(row.get("Name Arabic")),
        ]
        if name != ""
    ]

    person = None
    if row.get("Passport") or row.get("ID") or len(names) > 0:
        person = context.make("Person")
        person.id = context.make_id(row.get("ID") or row.get("Passport") or names[0])
        for name in row.pop("English Name").split(";"):
            h.apply_name(person, full=name, lang="eng")
        for name in row.pop("Name Hebrew").split(";"):
            h.apply_name(person, full=name, lang="heb")
        for name in row.pop("Name Arabic").split(";"):
            h.apply_name(person, full=name, lang="ara")
        for alias in row.pop("Hebrew aliases", "").split(";"):
            h.apply_name(person, full=alias, lang="heb", alias=True)
        for alias in row.pop("Arabic aliases", "").split(";"):
            h.apply_name(person, full=alias, lang="ara", alias=True)
        for alias in row.pop("English aliases", "").split(";"):
            h.apply_name(person, full=alias, lang="eng", alias=True)

        if id_number := row.pop("ID"):
            h.make_identification(
                context,
                person,
                id_number,
                passport=False,
                country=row.pop("ID Nationality"),
            )
        if passport_number := row.pop("Passport"):
            h.make_identification(
                context,
                person,
                passport_number,
                passport=True,
                country=row.pop("Passport Nationality"),
            )
        for email in row.pop("Email").split(";"):
            person.add("email", email)
        for phone in row.pop("Phone").split(";"):
            person.add("phone", phone)
        h.apply_date(person, "birthDate", row.pop("DOB (dd/mm/yyyy)"))
        context.emit(person)
    else:
        # There are some rows that only have an email (and not a passport number or ID),
        # those we do not emit as a Person
        row.pop("Email")

    # Get the wallets and binance accounts as a list
    wallets = []
    if exchange_accounts := row.pop("Account ID"):
        platform = row.pop("Account platform")
        for account_id in h.multi_split(exchange_accounts, [";"]):
            wallet = context.make("CryptoWallet")
            wallet.id = context.make_id(account_id)
            wallet.set("mangingExchange", platform)
            wallet.set("publicKey", account_id)
            wallets.append(wallet)
    if wallet_ids := row.pop("Wallet ID"):
        for wallet_id in h.multi_split(wallet_ids, [";"]):
            d = h.extract_cryptos(wallet_id)
            wallet = context.make("CryptoWallet")
            wallet.id = context.make_id(wallet_id)
            wallet.set("publicKey", wallet_id)
            wallets.append(wallet)
            if wallet_id in d:
                wallet.set("currency", d[wallet_id])

    # Create a sanction for each Crypto account
    aso_id = row.pop("Administrative Seizure Order")
    source_url = row.pop("sourceUrl")
    start_date = row.pop("Publication date")
    end_date = row.pop("ASO valid until")
    for wallet in wallets:
        wallet.set("holder", person)

        sanction = h.make_sanction(context, wallet, key=aso_id)
        sanction.set("authorityId", aso_id)
        h.apply_date(sanction, "startDate", start_date)
        h.apply_date(sanction, "endDate", end_date)
        if h.is_active(sanction):
            wallet.add("topics", "crime.terror")
        sanction.add("sourceUrl", source_url)
        context.emit(wallet)
        context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            # Just sequencing in the spreadsheet, not from the source.
            "#",
            # The wording is "the property of the organization, or used in financing
            # terrorism", i.e. something in the doc belongs to the org, but not
            # necessarily everything.
            "Organization",
        ],
    )


def crawl(context: Context):
    # Get a warning when a notice has been issued
    content_xpath = ".//main"
    doc = fetch_html(context, context.dataset.url, content_xpath, cache_days=1)
    container = doc.xpath(content_xpath)[0]

    # The key things to check are
    # - the table of releases - are there any new ones?
    # - The table of persons/wallets - does it look like anything's been added there?
    #   Top of bottom?
    #
    # Save As https://nbctf.mod.gov.il/en/Minister%20Sanctions/PropertyPerceptions/Pages/Blockchain1.aspx
    # as 'Web Page, HTML only' in chrome to datasets/il/mod_crypto/source.html
    # We use chrome because curl sometimes gets a bot blocking response.
    #
    # Then git diff --word-diff=color datasets/il/mod_crypto/source.html
    # and see if there's anything in the content that's changed.
    h.assert_dom_hash(container, "4232572bec290de579d19318c00e02c8e77cce38")

    # We don't support rowspan at the time of writing.
    #
    # table_xpath = ".//*[contains(text(), 'Full name')]//ancestor::table"
    # entity_table = doc.xpath(table_xpath)[0]
    # for row in h.parse_html_table(entity_table):
    #     print(row)

    # The CSV gives hand-extracted detail like names in arabic and hebrew.
    # And we're doing manual extraction to the google sheet until we support
    # rowspan in the table.
    src = context.fetch_resource("source.csv", context.data_url)
    with open(src, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            crawl_csv_row(context, row)
