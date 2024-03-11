from zavod import Context, helpers
import csv


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
    source_file = context.fetch_resource(
        "source.csv",
        context.dataset.data.url,
    )
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = context.make("Security")
            isin = row.pop("instrumentIdentifier")
            if isin is None:
                context.log.warn("No ISIN", row=row)
                return
            entity.id = f"isin-{isin}"
            entity.add("isin", isin)
            entity.add("name", row.pop("instrumentFullName", isin))

            sanction = helpers.make_sanction(context, entity)
            sanction.id = context.make_id(row.pop("id"))
            sanction.add("program", "ESMA")
            sanction.add("provisions", row.pop("actionType"))
            sanction.add("reason", row.pop("reasonsForTheAction"))
            sanction.add("description", row.pop("comments"))
            sanction.add("startDate", row.pop("effectiveFrom"))
            sanction.add("country", row.pop("memberStateOfNotifyingCA"))
            sanction.add("authority", row.pop("notifyingCA"))
            if row.get("effectiveTo") is not None:
                sanction.add("endDate", row.pop("effectiveTo"))
            else:
                entity.add("topic", "sanction")
            context.emit(sanction, target=False)

            if row.get("issuer") is not None:
                issuer = context.make("LegalEntity")
                issuer.id = context.make_id(row.pop("issuer"))
                issuer.add("leiCode", issuer)
                issuer.add("name", row.pop("issuerName"))
                context.emit(issuer, target=False)
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
                    "id",
                    "onGoing",
                ],
            )
