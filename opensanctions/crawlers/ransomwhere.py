import json
from pantomime.types import JSON

from opensanctions.core import Context
from opensanctions import helpers as h


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.dataset.data.url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for entry in data.get("result", []):
        wallet = context.make("CryptoWallet", target=True)
        wallet.id = context.make_slug(entry.get("address"))
        wallet.add("publicKey", entry.pop("address"))
        wallet.add("topics", "crime.theft")
        wallet.add("createdAt", entry.pop("createdAt"))
        wallet.add("modifiedAt", entry.pop("updatedAt"))
        wallet.add("name", entry.pop("family"))
        wallet.add("balance", entry.pop("balance"))
        wallet.add("amountUsd", entry.pop("balanceUSD"))
        wallet.add("currency", entry.pop("blockchain"))
        h.audit_data(entry, ignore=["transactions"])
        context.emit(wallet)
