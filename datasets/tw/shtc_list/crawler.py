import csv

from zavod import Context, helpers as h

ADDRESS_SPLITS = [
    ";",
    "i)",
    "ii)",
    "iii)",
    "iv)",
    "v)",
    "vi)",
    "vii)",
    "viii)",
]


def crawl(context: Context):
    path = context.fetch_resource("shtc_list.csv", context.data_url)
    with open(path, "rt", encoding="utf-8") as infh:
        for row in csv.DictReader(infh):
            row = {
                k.lstrip("\ufeff")
                .strip()
                .lower()
                .replace(" ", "_"): v.strip() if isinstance(v, str) else v
                for k, v in row.items()
            }
            item = row.pop("項次item")
            name = row.pop("名稱name")
            entity = context.make("LegalEntity")
            entity.id = context.make_id(item, name)
            for n in name.split(";"):
                entity.add("name", n)
            aliases = row.pop("別名alias")
            for alias in aliases.split(";"):
                entity.add("alias", alias.strip())
            countries = row.pop("國家代碼country_code")
            for country in countries.split(";"):
                entity.add("country", country.strip())
            addresses = row.pop("地址address")
            for address in h.multi_split(addresses, ADDRESS_SPLITS):
                entity.add("address", address.strip())
            entity.add("registrationNumber", row.pop("護照號碼id_number"))
            entity.add("topics", "debarment")
            context.emit(entity)
            context.audit_data(row)
