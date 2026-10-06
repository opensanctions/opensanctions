import csv  # TODO: Remove after the rekey run (see rekey_seizures_csv)
import re
import unicodedata  # TODO: Remove after the rekey run (see rekey_seizures_csv)
from pathlib import Path  # TODO: Remove after the rekey run (see rekey_seizures_csv)
from typing import Any

import orjson
from rigour.mime.types import JSON
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
HOLDER_PROPS = [
    "designatedOperatives",
    "nonDesignatedOperatives",
    "designatedOrganizations",
    "nonDesignatedOrganizations",
]
# Stand-in name for holders identified only by an ID number
UNKNOWN_NAME = "Unknown_Name_User"
# "Anonymous", given as the holder of wallets whose holder isn't known
ANONYMOUS = "אנונימי"
CURRENCY_CODE = re.compile(r"^[A-Z]{2,5}$")


def export_items(context: Context, name: str, items: Any) -> None:
    path = context.get_resource_path(f"{name}.json")
    path.write_bytes(orjson.dumps(items))
    context.export_resource(path, JSON, title=f"{context.SOURCE_TITLE} ({name})")


def operative_key(props: dict[str, Any], name: str | None) -> str | None:
    """The value the holder's ID is made from: an ID document number, a passport
    number or the name, in that order, as the crawler did before switching to the
    API."""
    for block in props["identificationDocuments"] or []:
        number = block["properties"]["identificationNumber"]
        if number is not None:
            return str(number)
    for block in props["passportDetails"] or []:
        number = block["properties"]["passportNumber"]
        if number is not None and number != "Unknown":
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


def crawl(context: Context) -> None:
    orders = fetch_content(context, "seizureAndForfeitureOrder", "en")
    wallets = fetch_content(context, "cryptocurrencyWallet", "en")
    operatives = fetch_variants(context, "operative")
    orgs = fetch_variants(context, "organization")

    # Map wallet IDs to the Administrative Seizure Orders (ASO)
    # and Forfeiture Orders (FO) they are involved in.
    # wallet_orders["ca5ce146-…"] == [<FO 55/23>, <FO 19/23>, <FO 56/23>]
    wallet_orders: dict[str, list[Item]] = {}
    for order in orders.values():
        for asset in order["properties"]["assets"] or []:
            if asset["id"] in wallets:
                wallet_orders.setdefault(asset["id"], []).append(order)

    holders: dict[str, Entity] = {}
    holder_variants: dict[str, list[Item]] = {}
    # TODO: Remove after the rekey run (see rekey_seizures_csv)
    # Wallet address -> (wallet ID, holder IDs), for rekeying
    new_ids: dict[str, tuple[str, list[str]]] = {}
    for wallet_item in wallets.values():
        wallet_props = dict(wallet_item["properties"])
        if wallet_props.pop("assetType") != "Crypto Wallet":
            raise ValueError(f"Unexpected asset type: {wallet_item}")
        wallet_holders: list[Entity] = []
        for holder_prop in HOLDER_PROPS:
            for holder_ref in wallet_props.pop(holder_prop) or []:
                if holder_ref["name"] == ANONYMOUS:
                    continue
                if holder_ref["id"] not in holders:
                    if holder_ref["id"] in operatives:
                        holder_variants[holder_ref["id"]] = operatives[holder_ref["id"]]
                        holders[holder_ref["id"]] = make_operative(
                            context, operatives[holder_ref["id"]]
                        )
                    else:
                        holder_variants[holder_ref["id"]] = orgs[holder_ref["id"]]
                        holders[holder_ref["id"]] = make_organization(
                            context, orgs[holder_ref["id"]]
                        )
                wallet_holders.append(holders[holder_ref["id"]])
        # TODO: Remove after the rekey run (see rekey_seizures_csv)
        holder_ids = [holder.id for holder in wallet_holders if holder.id is not None]

        coin = wallet_props.pop("coinType")
        # A single item can give an Ethereum and a TRON address, comma-separated
        for address in h.multi_split(wallet_props.pop("walletAddress"), [", "]):
            wallet = context.make("CryptoWallet")
            wallet.id = context.make_id(address)
            assert wallet.id is not None
            apply_address(context, wallet, address, coin)
            wallet.add("holder", wallet_holders)
            wallet.add("sourceUrl", source_url(wallet_item))
            # TODO: Remove after the rekey run (see rekey_seizures_csv)
            new_ids[address] = (wallet.id, holder_ids)

            # Create a sanction for each Order
            for order in wallet_orders.get(wallet_item["id"], []):
                order_props = order["properties"]
                sanction = h.make_sanction(context, wallet, key=order_key(order))
                sanction.add("authorityId", order["name"])
                sanction.add("provisions", order_props["orderType"])
                apply_date(sanction, "startDate", order_props["orderDate"])
                apply_date(sanction, "endDate", order_props["validityDate"])
                sanction.add("sourceUrl", source_url(order))
                if h.is_active(sanction):
                    wallet.add("topics", "crime.terror")
                context.emit(sanction)
            if wallet_item["id"] not in wallet_orders:
                context.log.warning("Wallet without order", address=address)
            context.emit(wallet)
        context.audit_data(wallet_props)

    for holder in holders.values():
        context.emit(holder)

    for order in orders.values():
        wallet_props = dict(order["properties"])
        for key in ("orderType", "orderNumber", "orderDate", "validityDate", "assets"):
            wallet_props.pop(key)
        if wallet_props.pop("isHidden") is not False:
            context.log.warning("Hidden order", order=order["name"])
        context.audit_data(
            wallet_props,
            ignore=[
                # Also given on the wallets, but the order lists holders of all
                # seized assets
                *HOLDER_PROPS,
            ],
        )

    export_items(
        context,
        "wallets",
        {"wallets": wallets, "orders": orders, "holders": holder_variants},
    )
    # TODO: Remove after the rekey run (see rekey_seizures_csv)
    rekey_seizures_csv(context, new_ids)


# TODO: Remove everything below, seizures.csv, and the code marked with TODOs above
# once this has run in production.
# Before switching to the API, the crawler read seizures.csv, maintained by hand
# from the old NBCTF site. Its values were obfuscated with homoglyphs and invisible
# characters, so some of the IDs made from them differ from the IDs made from the
# API. Map the old IDs to the new ones by joining on the wallet address.
SEIZURES_CSV = Path(__file__).parent / "seizures.csv"
ADDRESS_TAG = re.compile(r" \(Address tag \d+\)$")
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


def rekey_seizures_csv(
    context: Context, new_ids: dict[str, tuple[str, list[str]]]
) -> None:
    by_key = {join_key(addr): ids for addr, ids in new_ids.items()}
    # seizures.csv gives one Stellar address without its tag
    for addr, addr_ids in new_ids.items():
        by_key.setdefault(join_key(ADDRESS_TAG.sub("", addr)), addr_ids)
    # Old ID -> candidate new IDs, one set per seizures.csv row
    candidates: dict[str, list[set[str]]] = {}
    with open(SEIZURES_CSV) as fh:
        for row in csv.DictReader(fh):
            # As the old crawler read the row
            row = {k: v.replace("\u200b", "") for k, v in row.items()}
            identifier = row["wallet_address"] or row["account_id"]
            if identifier == "":
                continue
            old_wallet_id = context.make_id(normalize_address(identifier))
            old_holder_id = None
            name = row["name"] or None
            if row["schema"] == "Person":
                old_holder_id = context.make_id(
                    row["id_no"] or row["passport_no"] or name
                )
            elif row["schema"] == "LegalEntity":
                old_holder_id = context.make_id(name)

            # Some Binance account numbers in the API are in the phone column of
            # seizures.csv, which has other numbers as the account.
            ids = by_key.get(join_key(identifier)) or by_key.get(join_key(row["phone"]))
            if ids is None:
                context.log.info("No API wallet for seizures.csv row", id=identifier)
                continue
            new_wallet_id, new_holder_ids = ids
            assert old_wallet_id is not None
            candidates.setdefault(old_wallet_id, []).append({new_wallet_id})
            if old_holder_id is not None and len(new_holder_ids) > 0:
                candidates.setdefault(old_holder_id, []).append(set(new_holder_ids))

    for old_id, sets in candidates.items():
        # Some wallets have two holders, so narrow down to the holder common to
        # all the wallets of the old entity.
        if any(old_id in new for new in sets):
            continue
        common = set.intersection(*sets)
        if len(common) != 1:
            context.log.info("Not rekeying ambiguous ID", old_id=old_id, new=common)
            continue
        context.rekey(old_id, common.pop())
