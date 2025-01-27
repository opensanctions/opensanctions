from typing import Dict
import re

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("unvan")
    if not name:
        return

    # Clean the name by removing numbers in parentheses
    name = re.sub(r"\s*\(\d+\)\s*$", "", name)

    entity = context.make("Person")
    entity.id = context.make_id(row.pop("kisiId"))
    entity.add("name", name)
    entity.add("country", "tr")

    # Add MKK registration number if available
    if mkkNo := row.pop("mkkSicilNo"):
        entity.add("registrationNumber", mkkNo)


    # Create sanction
    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", row.pop("kurulKararNo"))
    sanction.add("status", row.pop("yargilamaAsamasi"))

    # Add information about the related stock
    if stock_name := row.pop("pay"):
        sanction.add("description", f"Related stock: {stock_name}")
    if stock_code := row.pop("payKodu"):
        sanction.add("description", f"Stock code: {stock_code}")

    # Add decision details
    if decision_date := row.pop("kurulKararTarihi"):
        h.apply_date(sanction, "startDate", decision_date)

    # Add the reason/explanation
    if explanation := row.pop("aciklama"):
        sanction.add("reason", explanation)

    entity.add("topics", "sanction")

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ignore=["id", "kurulKararTarihiStr", "davaBilgisi"])


def crawl(context: Context) -> None:
    for item in context.fetch_json(context.data_url):
        crawl_item(item, context)
