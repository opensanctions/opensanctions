import json
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h


def format_number(value):
    if value is not None:
        return "%.2f" % float(value)


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for entry in data.get("result", []):
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_slug(entry.get("address"))
        wallet.add("publicKey", entry.pop("address"))
        wallet.add("topics", "crime.theft")
        wallet.add("createdAt", entry.pop("createdAt"))
        wallet.add("modifiedAt", entry.pop("updatedAt"))
        wallet.add("alias", entry.pop("family"))
        wallet.add("balance", format_number(entry.pop("balance")))
        wallet.add("amountUsd", format_number(entry.pop("balanceUSD")))
        wallet.add("currency", entry.pop("blockchain"))
        context.audit_data(entry, ignore=["transactions"])
        context.emit(wallet, target=True)
