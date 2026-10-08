import re
from typing import Any

from zavod.entity import Entity
from zavod.shed.il_mod import (
    Item,
    apply_date,
    apply_operative_details,
    drop_fallbacks,
    fetch_content,
    fetch_variants,
    is_placeholder,
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
CURRENCY_CODE = re.compile(r"^[A-Z]{2,5}$")


def operative_key(props: dict[str, Any], name: str | None) -> str | None:
    """The value the holder's ID is made from: an ID document number, a passport
    number or the name, in that order, as the crawler did before switching to the
    API."""
    for block in props["identificationDocuments"] or []:
        number = block["properties"]["identificationNumber"]
        if not is_placeholder(number):
            return str(number)
    for block in props["passportDetails"] or []:
        number = block["properties"]["passportNumber"]
        if not is_placeholder(number):
            return str(number)
    return name


def make_operative(context: Context, variants: list[Item]) -> Entity | None:
    """Make the holder Person, or None for a placeholder such as "אנונימי"
    (anonymous), given for wallets whose holder isn't known."""
    item = variants[0]
    names = drop_fallbacks(variants, "fullName")
    # e.g. "Unknown_Name_User" for holders identified only by an ID number
    name_en, name_he, name_ar = [None if is_placeholder(n) else n for n in names]
    props = dict(item["properties"])
    entity = context.make("Person")
    # Using name remaining after removing to avoid unintentional merges.
    entity.id = context.make_id(operative_key(props, name_en or name_he or name_ar))
    if entity.id is None:
        return None
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


def apply_address(context: Context, wallet: Entity, address: str, coin: str) -> None:
    res = context.lookup("coin_type", coin)
    if res is not None:
        wallet.add("managingExchange", res.exchange)
    elif CURRENCY_CODE.match(coin) is None:
        context.log.warning("Unknown coin type", coin=coin, address=address)
    # Exchange account numbers are given where a wallet address would be, and
    # not always with the exchange as the coin type. The coin type of such an
    # account isn't the currency of the account, so is only used for addresses.
    if (res is not None and res.account) or address.isdigit():
        wallet.add("accountId", address)
        return
    wallet.add("publicKey", address)
    wallet.add("currency", res.currency if res is not None else coin)


def crawl_wallet(
    context: Context,
    wallet_item: Item,
    order: Item,
    operatives: dict[str, list[Item]],
    orgs: dict[str, list[Item]],
) -> None:
    """Emit the wallets of a wallet item, with their holders and a sanction for
    the order."""
    wallet_props = dict(wallet_item["properties"])
    if wallet_props.pop("assetType") != "Crypto Wallet":
        raise ValueError(f"Unexpected asset type: {wallet_item}")
    wallet_holders: list[Entity] = []
    for holder_prop in WALLET_HOLDER_PROPS:
        for holder_ref in wallet_props.pop(holder_prop) or []:
            holder_id = holder_ref["id"]
            if holder_id in operatives:
                holder = make_operative(context, operatives[holder_id])
            else:
                holder = make_organization(context, orgs[holder_id])
            if holder is None:
                continue
            context.emit(holder)
            wallet_holders.append(holder)

    order_props = order["properties"]
    coin = wallet_props.pop("coinType")
    # A single item can give an Ethereum and a TRON address, comma-separated
    for address in h.multi_split(wallet_props.pop("walletAddress"), [", "]):
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(address)
        assert wallet.id is not None
        apply_address(context, wallet, address, coin)
        wallet.add("holder", wallet_holders)
        wallet.add("sourceUrl", source_url(wallet_item))

        sanction = h.make_sanction(
            context, wallet, key=order["properties"]["orderNumber"]
        )
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

    for order in orders.values():
        crawl_order(context, order, wallets, operatives, orgs)
