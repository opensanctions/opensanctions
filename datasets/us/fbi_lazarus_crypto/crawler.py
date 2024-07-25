import csv
from typing import Dict
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    publicKey = row.pop("Address")
    network = row.pop("Network")
    linked_to = row.pop("Linked to")

    entity = None
    entity = context.make("CryptoWallet")
    entity.id = context.make_id(publicKey, network)
    entity.add("publicKey", publicKey)
    entity.add("notes", network)
    entity.add("holder", linked_to)
    entity.add("topics", "crime.fin")

    context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
