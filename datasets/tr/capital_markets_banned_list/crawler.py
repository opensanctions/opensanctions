from typing import Dict
import re

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("unvan")
    if not name:
        return

    # Clean the name by removing numbers in parentheses
    # The number means the number of cases against the person
    name = re.sub(r"\s*\(\d+\)\s*$", "", name)

    entity = context.make("Person")
    entity.id = context.make_id(row.pop("kisiId"))
    entity.add("name", name)
    entity.add("country", "tr")

    entity.add("registrationNumber", row.pop("mkkSicilNo"))

    # Create sanction
    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", row.pop("kurulKararNo"))
    sanction.add("status", row.pop("yargilamaAsamasi"))

    # Add information about the related stock
    stock_code = row.pop("payKodu")
    stock_name = row.pop("pay")
    if stock_code and stock_name:
        sanction.add("description", f"Due to activity relating to {stock_code} ({stock_name})")

    # Add decision details
    if decision_date := row.pop("kurulKararTarihi"):
        h.apply_date(sanction, "startDate", decision_date)

    sanction.add("reason", row.pop("aciklama"))

    entity.add("topics", "reg.action")

    context.emit(entity, target=True)
    context.emit(sanction)

    # id = internal id
    # kurulKararTarihiStr (decision date) as string (we already have decision date)
    # davaBilgisi (Case Information) not used because it's always "Yok" (No) or "Var" (Yes)
    if row.get("davaBilgisi") and row.get("davaBilgisi") not in ["Yok", "Var"]:
        context.log.warning("Dava bilgisi var: %s", row)
    context.audit_data(row, ignore=["id", "kurulKararTarihiStr", "davaBilgisi"])


def crawl(context: Context) -> None:
    for item in context.fetch_json(context.data_url):
        crawl_item(item, context)
