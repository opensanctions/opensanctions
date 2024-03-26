import csv

from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    # Set the initial cookie
    data = {
        "core": "esma_registers_saris_new",
        "pagingSize": "10",
        "start": 0,
        "keyword": "",
        "sortField": "effectiveFrom asc",
        "criteria": [],
        "wt": "json",
    }
    context.http.post(
        "https://registers.esma.europa.eu/publication/searchRegister/doMainSearch",
        json=data,
    )
    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            isin = row.pop("instrumentIdentifier")
            if isin is None:
                context.log.warn("No ISIN", row=row)
                return
            entity = h.make_security(context, isin)
            entity.add("name", row.pop("instrumentFullName", isin))

            sanction = h.make_sanction(context, entity, key=row.pop("id"))
            sanction.add("program", "ESMA")
            sanction.add("provisions", row.pop("actionType"))
            reason = row.pop("reasonsForTheAction")
            sanction.add("reason", reason)
            sanction.add("description", row.pop("comments"))
            sanction.add("startDate", row.pop("effectiveFrom"))
            sanction.add("country", row.pop("memberStateOfNotifyingCA"))
            sanction.set("authority", row.pop("notifyingCA"))
            end_date = row.pop("effectiveTo")
            sanction.add("endDate", end_date)
            if end_date:
                entity.add("topics", "poi")
            else:
                topic = context.lookup_value("reason_topic", reason)
                if topic is None:
                    context.log.warn("No topic defined for reason", reason=reason)
                entity.add("topics", topic)
            context.emit(sanction)
            issuer_id = row.pop("issuer", "").strip()
            if issuer_id != "":
                issuer = context.make("LegalEntity")
                if len(issuer_id) == 20:
                    issuer.id = f"lei-{issuer_id}"
                    issuer.add("leiCode", issuer_id)
                else:
                    issuer.id = context.make_id(issuer_id)
                issuer.add("name", row.pop("issuerName"))
                context.emit(issuer)
                entity.add("issuer", issuer)

            context.emit(entity, target=True)
            context.audit_data(
                row,
                ignore=[
                    "sufficientlyRelatedInstrument",
                    "otherRelatedInstrument",
                    "historicalStatus",
                    "markets",
                    "timestamp",
                    "onGoing",
                ],
            )
