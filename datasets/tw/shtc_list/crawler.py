import csv

from zavod import Context, helpers as h

ADDRESS_SPLITS = [
    ";",
    "iii)",
    "ii)",
    "i)",
    "iv)",
    "v)",
    "vi)",
    "viii)",
    "vii)",
    "Branch Office 1:",
    "Branch Office 2:",
    "Branch Office 3:",
    "Branch Office 4:",
    "Branch Office 5:",
    "Branch Office 6:",
    "Branch Office 7:",
    "Branch Office 8:",
    "Branch Office 9:",
    "Branch Office 10:",
    "Branch Office 11:",
    "Branch Office 12:",
    "Branch Office 13:",
    "Branch Office 14:",
    "Branch Office 15:",
    "Branch Office 16:",
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
            names = row.pop("名稱name")
            aliases = row.pop("別名alias")
            if not any([names, aliases]):
                continue
            entity = context.make("LegalEntity")
            entity.id = context.make_id(names, aliases)
            for name in names.split(";"):
                entity.add("name", name)
            for alias in aliases.split(";"):
                entity.add("alias", alias)
            for country in row.pop("國家代碼country_code").split(";"):
                entity.add("country", country)
            for address in h.multi_split(row.pop("地址address"), ADDRESS_SPLITS):
                entity.add("address", address)
            for id in row.pop("護照號碼id_number").split(";"):
                entity.add("idNumber", id)
            entity.add("topics", "debarment")

            context.emit(entity)
            context.audit_data(row, ["項次item"])
