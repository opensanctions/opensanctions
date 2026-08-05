import csv
from rigour.mime.types import CSV

from zavod import Context, helpers as h


def crawl_row(context: Context, row: dict[str, str]) -> None:
    mine_name = row.pop("Mine_Name_English")
    mine_name_zh = row.pop("Mine_Name_Chinese")
    company_name = row.pop("Company_Name_English")
    entity = context.make("Company")
    entity.id = context.make_id(mine_name, mine_name_zh, company_name)
    entity.add("name", mine_name, lang="eng")
    entity.add("name", mine_name_zh, lang="zho")
    entity.add("classification", row.pop("Mine_Type"))
    entity.add("topics", "export.risk")
    entity.add("topics", "forced.labor")

    company_name_zh = row.pop("Company_Name_Chinese")
    owner = context.make("Company")
    owner.id = context.make_id(company_name, company_name_zh)
    owner.add("name", company_name, lang="eng")
    owner.add("name", company_name_zh, lang="zho")

    own = context.make("Ownership")
    own.id = context.make_id(entity.id, owner.id)
    own.add("owner", owner.id)
    own.add("asset", entity.id)
    own.add("recordId", row.pop("Permit_Number"))
    h.apply_date(own, "endDate", row.pop("Permit_Expiration_Date"))

    context.emit(entity)
    context.emit(owner)
    context.emit(own)
    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
