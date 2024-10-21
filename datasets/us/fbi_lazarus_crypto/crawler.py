import csv
from typing import Dict
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    publicKey = row.pop("Address")
    network = row.pop("Network")
    linked_to = row.pop("Linked to")

    # Create and emit the Organization entity if linked_to is provided
    if linked_to:
        organization = context.make("Organization")
        organization.id = context.make_slug(linked_to)
        organization.add("name", linked_to)
        organization.add("topics", "crime.fin")
        organization.add("topics", "crime.cyber")
        context.emit(organization)

    # Create and emit the CryptoWallet entity
    wallet = context.make("CryptoWallet")
    wallet.id = context.make_id(publicKey, network)
    wallet.add("publicKey", publicKey)
    wallet.add("currency", network)
    if linked_to:
        wallet.add("holder", organization.id)  # Link the wallet to the organization
    wallet.add("topics", "crime.fin")
    wallet.add("topics", "crime.cyber")

    context.emit(wallet)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
