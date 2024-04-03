from zavod import Context
from zavod import helpers as h
import csv


def crawl(context: Context):
    src = context.fetch_resource("source.csv", context.data_url)
    with open(src, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Create a Person entity if present in the data
            names = [
                name
                for name in [
                    row.get("English Name"),
                    row.get("Name Hebrew"),
                    row.get("Name Arabic"),
                ]
                if name != ""
            ]
            # If there is a name, create a person entity
            person = None
            if len(names) > 0:
                person = context.make("Person")
                person.id = context.make_id(
                    row.get("ID") or row.get("Passport") or names[0]
                )
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
                context.emit(person)
            # Get the wallets and binance accounts as a list
            wallets = []
            if binance_accounts := row.pop("Binance client account"):
                for binance in binance_accounts.replace(" ", "").split(";"):
                    wallet = context.make("CryptoWallet")
                    wallet.id = context.make_id(binance)
                    wallet.set("mangingExchange", "Binance")
                    wallet.set("publicKey", binance)
                    wallets.append(wallet)
            if wallet_ids := row.pop("Wallet ID"):
                for wallet_id in wallet_ids.replace(" ", "").split(";"):
                    d = h.extract_cryptos(wallet_id)
                    wallet = context.make("CryptoWallet")
                    wallet.id = context.make_id(wallet_id)
                    wallet.set("publicKey", wallet_id)
                    wallets.append(wallet)
                    if wallet in d:
                        wallet.set("currency", d[wallet])
            aso_id = row.pop("Administrative Seizure Order")
            # Create a sanction for each Crypto account
            for wallet in wallets:
                wallet.set("holder", person)
                wallet.set("topics", "crime.terror")
                sanction = h.make_sanction(context, wallet)
                sanction.set("authorityId", aso_id)
                sanction.set(
                    "startDate",
                    h.parse_date(row.get("Publication date", ""), formats=["%m/%d/%Y"]),
                )
                context.emit(wallet, target=True)
