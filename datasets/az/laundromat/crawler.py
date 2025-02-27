from zavod import Context


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url)
    for row in data["data"]:
        # Payer info
        payer_schema = row.pop("payer_type")
        if payer_schema not in ("Company", "Person"):
            payer_schema = "LegalEntity"
        payer = context.make(payer_schema)
        payer_name = row.pop("payer_name")
        payer_name_norm = row.pop("payer_name_norm")
        payer.id = context.make_id(
            "entity",
            payer_name_norm,
            row["payer_jurisdiction"],
            row["payer_account"],
        )
        payer.add("name", payer_name)
        if payer_name != payer_name_norm:
            payer.add("alias", payer_name_norm)
        payer.add("jurisdiction", row.pop("payer_jurisdiction"))
        payer.add("country", row.pop("payer_bank_country"))
        payer.add("idNumber", row.pop("payer_account"))
        if row.pop("payer_core", False):
            payer.add("topics", "crime.fin")
        context.emit(payer)

        # Beneficiary info
        beneficiary_schema = row.pop("beneficiary_type")
        if beneficiary_schema not in ("Company", "Person"):
            beneficiary_schema = "LegalEntity"
        beneficiary = context.make(beneficiary_schema)
        beneficiary_name = row.pop("beneficiary_name")
        beneficiary_name_norm = row.pop("beneficiary_name_norm")
        beneficiary.id = context.make_id(
            "entity",
            beneficiary_name_norm,
            row["beneficiary_jurisdiction"],
            row["beneficiary_account"],
        )
        beneficiary.add("name", beneficiary_name)
        if beneficiary_name != beneficiary_name_norm:
            beneficiary.add("alias", beneficiary_name_norm)
        beneficiary.add("jurisdiction", row.pop("beneficiary_jurisdiction"))
        beneficiary.add("country", row.pop("beneficiary_bank_country"))
        beneficiary.add("idNumber", row.pop("beneficiary_account"))
        if row.pop("beneficiary_core", False):
            beneficiary.add("topics", "crime.fin")
        context.emit(beneficiary)

        payment = context.make("Payment")
        date = row.pop("date")
        payment.id = context.make_id(
            "payment",
            row.pop("id"),
            date,
            beneficiary_name_norm,
            payer_name_norm,
        )
        payment.add("date", date)
        payment.add("amount", row.pop("amount_orig"))
        payment.add("amountUsd", row.pop("amount_usd"))
        payment.add("amountEur", row.pop("amount_eur"))
        payment.add("currency", row.pop("amount_orig_currency"))
        payment.add("purpose", row.pop("purpose"))

        context.audit_data(row, ignore=["investigation", "source_file"])
