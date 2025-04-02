import json
from typing import Dict
import re

from rigour.mime.types import JSON

from zavod import Context, helpers as h

REGEX_MASK = re.compile(r"(\d+)\*+")


def crawl_item(row: Dict[str, str], context: Context):
    name = row.pop("unvan")
    if not name:
        return

    # Clean the name by removing numbers in parentheses
    # The number means the number of cases against the person
    name = re.sub(r"\s*\(\d+\)\s*$", "", name)
    # MKK Registration Number
    # MKK (https://www.mkk.com.tr/en/) is the central registry and securities
    # depository of Türkiye who is responsible for the central custody and
    # dematerialization* of capital market instruments
    mkk_number = row.pop("mkkSicilNo")
    # They seem to randomise mask length so let's trim that to avoid daily modifications
    mkk_number = REGEX_MASK.sub(r"\1***", mkk_number)

    entity = context.make("Person")
    entity.id = context.make_id(name, mkk_number)
    entity.add("name", name)
    entity.add("country", "tr")
    entity.add("topics", "reg.action")
    entity.add("idNumber", mkk_number)

    # Create sanction
    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", row.pop("kurulKararNo"))
    sanction.add("status", row.pop("yargilamaAsamasi"))

    # Add information about the related stock
    stock_code = row.pop("payKodu")
    stock_name = row.pop("pay")
    if stock_code and stock_name:
        sanction.add(
            "description", f"Due to activity relating to {stock_code} ({stock_name})"
        )

    # Add decision details
    if decision_date := row.pop("kurulKararTarihi"):
        h.apply_date(sanction, "startDate", decision_date)

    sanction.add("reason", row.pop("aciklama"))

    context.emit(entity)
    context.emit(sanction)

    if row.get("davaBilgisi") and row.get("davaBilgisi") not in ["Yok", "Var"]:
        context.log.warning("Dava bilgisi var: %s" % row)
    context.audit_data(
        row,
        ignore=[
            "id",
            "kisiId",  # seems to change daily.
            "kurulKararTarihiStr",  # (decision date) as string (we already have decision date)
            "davaBilgisi",  # (Case Information) not used because it's always "Yok" (No) or "Var" (Yes)
        ],
    )


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as file:
        data = json.load(file)
    for item in data:
        crawl_item(item, context)
