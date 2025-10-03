import csv
from typing import Dict
from rigour.mime.types import CSV

from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    mine_name = row.pop("Mine_Name_English")
    mine_name_zh = row.pop("Mine_Name_Chinese")
    entity = context.make("Organization")
    entity.id = context.make_id(mine_name, mine_name_zh)
    entity.add("name", mine_name, lang="eng")
    entity.add("name", mine_name_zh, lang="zho")
    entity.add("classification", row.pop("Mine_Type"))
    entity.add("topics", "export.risk")
    entity.add("topics", "forced.labor")
    context.emit(entity)

    company_name = row.pop("Company_Name_English")
    company_name_zh = row.pop("Company_Name_Chinese")
    owner = context.make("Company")
    owner.id = context.make_id(company_name, company_name_zh)
    owner.add("name", company_name, lang="eng")
    owner.add("name", company_name_zh, lang="zho")
    context.emit(owner)

    own = context.make("Ownership")
    own.id = context.make_id(entity.id, owner.id)
    own.add("owner", owner.id)
    own.add("asset", entity.id)
    own.add("recordId", row.pop("Permit_Number"))
    h.apply_date(own, "endDate", row.pop("Permit_Expiration_Date"))
    context.emit(own)

    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
