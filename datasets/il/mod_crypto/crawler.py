import csv  # TODO: Remove after the rekey run (see rekey_wallet)
import re
import unicodedata  # TODO: Remove after the rekey run (see rekey_wallet)
from pathlib import Path  # TODO: Remove after the rekey run (see rekey_wallet)
from typing import Any

from zavod.entity import Entity
from zavod.shed.il_mod import (
    Item,
    apply_date,
    apply_operative_details,
    drop_fallbacks,
    fetch_content,
    fetch_variants,
    source_url,
)

from zavod import Context
from zavod import helpers as h

# The holders of a wallet or account, as given on wallets
WALLET_HOLDER_PROPS = [
    "designatedOperatives",
    "nonDesignatedOperatives",
    "designatedOrganizations",
    "nonDesignatedOrganizations",
]
# Stand-in name for holders identified only by an ID number
UNKNOWN_NAME = "Unknown_Name_User"
# "Anonymous", given as the holder of wallets whose holder isn't known
ANONYMOUS = "אנונימי"
# Given as ID or passport numbers when there is none
NO_NUMBER = {"Unknown", "NOT AVAILABLE"}
CURRENCY_CODE = re.compile(r"^[A-Z]{2,5}$")


def operative_key(props: dict[str, Any], name: str | None) -> str | None:
    """The value the holder's ID is made from: an ID document number, a passport
    number or the name, in that order, as the crawler did before switching to the
    API."""
    for block in props["identificationDocuments"] or []:
        number = block["properties"]["identificationNumber"]
        if number is not None and number not in NO_NUMBER:
            return str(number)
    for block in props["passportDetails"] or []:
        number = block["properties"]["passportNumber"]
        if number is not None and number not in NO_NUMBER:
            return str(number)
    return name


def make_operative(context: Context, variants: list[Item]) -> Entity:
    item = variants[0]
    names = drop_fallbacks(variants, "fullName")
    name_en, name_he, name_ar = [None if n == UNKNOWN_NAME else n for n in names]
    props = dict(item["properties"])
    entity = context.make("Person")
    entity.id = context.make_id(operative_key(props, name_en))
    if entity.id is None:
        raise ValueError(f"Holder without name or ID: {item['id']}")
    props.pop("fullName")
    entity.add("name", name_en, lang="eng")
    entity.add("name", name_he, lang="heb")
    entity.add("name", name_ar, lang="ara")
    entity.add("sourceUrl", source_url(item))
    apply_operative_details(context, entity, props, None)
    context.audit_data(
        props,
        ignore=[
            "isDesignated",
            # Set on holders who aren't designated, with no explanation
            "temporaryDesignationDate",
        ],
    )
    return entity


def make_organization(context: Context, variants: list[Item]) -> Entity:
    item = variants[0]
    name_en, name_he, name_ar = drop_fallbacks(variants, "organizationName")
    props = dict(item["properties"])
    props.pop("organizationName")
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name_en, name_he)
    if entity.id is None:
        raise ValueError(f"Holder without name: {item['id']}")
    entity.add("name", name_en, lang="eng")
    entity.add("name", name_he, lang="heb")
    entity.add("name", name_ar, lang="ara")
    entity.add("sourceUrl", source_url(item))
    context.audit_data(props, ignore=["isDesignated"])
    return entity


def order_key(order: Item) -> str:
    """The key of the sanction ID for an order.

    Orders followed by forfeiture are listed as "FO", but keep the number and date
    of the original seizure order (ASO). The crawler keyed sanctions as "ASO 5/24"
    before switching to the API."""
    number, year = order["properties"]["orderNumber"].split("/")
    return f"ASO {int(number)}/{year}"


def apply_address(context: Context, wallet: Entity, address: str, coin: str) -> None:
    res = context.lookup("coin_type", coin)
    if res is not None:
        wallet.add("managingExchange", res.exchange)
        wallet.add("currency", res.currency)
    elif CURRENCY_CODE.match(coin) is not None:
        wallet.add("currency", coin)
    else:
        context.log.warning("Unknown coin type", coin=coin, address=address)
    # Exchange account numbers are given where a wallet address would be, and
    # not always with the exchange as the coin type.
    if (res is not None and res.account) or address.isdigit():
        wallet.add("accountId", address)
    else:
        wallet.add("publicKey", address)


def crawl_wallet(
    context: Context,
    wallet_item: Item,
    order: Item,
    operatives: dict[str, list[Item]],
    orgs: dict[str, list[Item]],
    # TODO: Remove after the rekey run (see rekey_wallet)
    old_wallet_ids: dict[str, set[str]],
) -> None:
    """Emit the wallets of a wallet item, with their holders and a sanction for
    the order."""
    wallet_props = dict(wallet_item["properties"])
    if wallet_props.pop("assetType") != "Crypto Wallet":
        raise ValueError(f"Unexpected asset type: {wallet_item}")
    wallet_holders: list[Entity] = []
    for holder_prop in WALLET_HOLDER_PROPS:
        for holder_ref in wallet_props.pop(holder_prop) or []:
            if holder_ref["name"] == ANONYMOUS:
                continue
            holder_id = holder_ref["id"]
            if holder_id in operatives:
                holder = make_operative(context, operatives[holder_id])
            else:
                holder = make_organization(context, orgs[holder_id])
            context.emit(holder)
            wallet_holders.append(holder)

    order_props = order["properties"]
    coin = wallet_props.pop("coinType")
    # A single item can give an Ethereum and a TRON address, comma-separated
    for address in h.multi_split(wallet_props.pop("walletAddress"), [", "]):
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(address)
        assert wallet.id is not None
        # TODO: Remove after the rekey run (see rekey_wallet)
        rekey_wallet(context, old_wallet_ids, address, wallet.id)
        apply_address(context, wallet, address, coin)
        wallet.add("holder", wallet_holders)
        wallet.add("sourceUrl", source_url(wallet_item))

        sanction = h.make_sanction(context, wallet, key=order_key(order))
        sanction.add("authorityId", order["name"])
        sanction.add("provisions", order_props["orderType"])
        apply_date(sanction, "startDate", order_props["orderDate"])
        apply_date(sanction, "endDate", order_props["validityDate"])
        sanction.add("sourceUrl", source_url(order))
        if h.is_active(sanction):
            wallet.add("topics", "crime.terror")
        context.emit(sanction)
        context.emit(wallet)
    context.audit_data(wallet_props)


def crawl_order(
    context: Context,
    order: Item,
    wallets: dict[str, Item],
    operatives: dict[str, list[Item]],
    orgs: dict[str, list[Item]],
    # TODO: Remove after the rekey run (see rekey_wallet)
    old_wallet_ids: dict[str, set[str]],
) -> None:
    """Emit the wallets seized by an Administrative Seizure Order (ASO) or
    Forfeiture Order (FO)."""
    order_props = dict(order["properties"])
    hidden = order_props.pop("isHidden")
    if hidden is True:
        context.log.warning("Skipping hidden order", order=order["name"])
        return
    if hidden is not False:
        raise ValueError(f"Unexpected isHidden: {hidden!r} ({order['name']})")
    # Assets also include other property, given as generalAsset items
    for asset in order_props.pop("assets") or []:
        if asset["id"] in wallets:
            crawl_wallet(
                context,
                wallets[asset["id"]],
                order,
                operatives,
                orgs,
                old_wallet_ids,
            )
    context.audit_data(
        order_props,
        ignore=[
            # Used in crawl_wallet
            "orderType",
            "orderNumber",
            "orderDate",
            "validityDate",
            # Also given on the wallets, but the order lists holders of all
            # seized assets
            *WALLET_HOLDER_PROPS,
        ],
    )


def crawl(context: Context) -> None:
    orders = fetch_content(context, "seizureAndForfeitureOrder", "en")
    wallets = fetch_content(context, "cryptocurrencyWallet", "en")
    operatives = fetch_variants(context, "operative")
    orgs = fetch_variants(context, "organization")
    # TODO: Remove after the rekey run (see rekey_wallet)
    old_wallet_ids = load_old_wallet_ids(context, wallets)

    for order in orders.values():
        crawl_order(context, order, wallets, operatives, orgs, old_wallet_ids)


# TODO: Remove everything below, seizures.csv, and the code marked with TODOs above
# once this has run in production.
# Before switching to the API, the crawler read seizures.csv, maintained by hand
# from the old NBCTF site. Its values were obfuscated with homoglyphs and invisible
# characters, so some of the wallet IDs made from them differ from the IDs made from
# the API. Map the old IDs to the new ones by joining on the wallet address.
#
# Holders aren't rekeyed: for some wallets the API gives a different holder than
# seizures.csv did, and rekeying those would merge different people.
SEIZURES_CSV = Path(__file__).parent / "seizures.csv"
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


def normalize_address(addr: str) -> str:
    return "".join(HOMOGLYPHS.get(c) or c for c in addr)


def join_key(value: str) -> str:
    """Strip invisible characters and homoglyphs, and the trailing asterisks the
    spreadsheet put on some addresses."""
    visible = "".join(c for c in value if unicodedata.category(c) != "Cf")
    return normalize_address(visible).rstrip("*").strip()


def load_old_wallet_ids(
    context: Context, wallets: dict[str, Item]
) -> dict[str, set[str]]:
    """Map the join keys of the wallet addresses in seizures.csv to the wallet IDs
    the old crawler made from them."""
    api_keys = {
        join_key(address)
        for item in wallets.values()
        for address in h.multi_split(item["properties"]["walletAddress"], [", "])
    }
    by_address: dict[str, set[str]] = {}
    by_phone: dict[str, set[str]] = {}
    with open(SEIZURES_CSV) as fh:
        for row in csv.DictReader(fh):
            # As the old crawler read the row
            row = {k: v.replace("\u200b", "") for k, v in row.items()}
            identifier = row["wallet_address"] or row["account_id"]
            if identifier == "":
                continue
            old_id = context.make_id(normalize_address(identifier))
            assert old_id is not None
            by_address.setdefault(join_key(identifier), set()).add(old_id)
            # Only rows whose own address isn't a wallet in the API: some people's
            # account and phone number are both given as wallets.
            if row["phone"] != "" and join_key(identifier) not in api_keys:
                by_phone.setdefault(join_key(row["phone"]), set()).add(old_id)
    # Some Binance account numbers in the API are in the phone column of
    # seizures.csv, which has other numbers as the account. Only use a phone
    # number if it's on a single account.
    for phone, old_ids in by_phone.items():
        if len(old_ids) == 1 and phone not in by_address:
            by_address[phone] = old_ids
    return by_address


def rekey_wallet(
    context: Context, old_wallet_ids: dict[str, set[str]], address: str, new_id: str
) -> None:
    for old_id in old_wallet_ids.get(join_key(address), set()):
        context.rekey(old_id, new_id)
