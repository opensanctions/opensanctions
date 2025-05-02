import csv
from typing import Dict

# from normality.cleaning import collapse_spaces

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ID_FIELDS = [("id_no", "id_country"), ("residency_no", "residency_country")]


def remove_zero_width_space(row):
    return {
        k: (v.replace("\u200b", "") if isinstance(v, str) else v)
        for k, v in row.items()
    }


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
        person.add("email", row.pop("email"))
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
    # TODO: startDate is to be extracted from the orders
    # start_date = row.pop("Publication date")
    for wallet in wallets:
        sanction = h.make_sanction(context, wallet, key=aso_id)
        sanction.set("authorityId", aso_id)
        # h.apply_date(sanction, "startDate", start_date)
        h.apply_date(sanction, "modifiedAt", row.pop("last_updated"))
        h.apply_date(sanction, "endDate", row.pop("end_date"))
        if h.is_active(sanction):
            wallet.add("topics", "crime.terror")
        sanction.add("sourceUrl", row.pop("order_url"))
        sanction.add("sourceUrl", row.pop("forfeiture_order_url"))
        sanction.add("sourceUrl", row.pop("annex_url"))
        context.emit(wallet)
        context.emit(sanction)

    context.audit_data(row)


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
    # as 'Web Page, HTML only' in chrome to a temporary location e.g. /tmp/il_crypto.html.
    # We use chrome because curl sometimes gets a bot blocking response.
    #
    # Then run `xmllint --format --html /tmp/il_crypto.html > datasets/il/mod_crypto/source.html`
    # Then run `dos2unix datasets/il/mod_crypto/source.html`
    # Then git diff --word-diff=color datasets/il/mod_crypto/source.html
    # and see if there's anything in the content that's changed.
    #
    # Commit the changes so that we can see what changed from here next time.
    h.assert_dom_hash(container, "203b99615f06e11bf4af3273e2cb46506c0804c4")

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
            row = remove_zero_width_space(row)
            crawl_csv_row(context, row)
