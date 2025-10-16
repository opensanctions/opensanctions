from zavod import Context
from zavod import helpers as h

STATIC_URL = "https://data.opensanctions.org/contrib/iso9362_bic/20241021/ISOBIC.pdf"
EXTRACT_ARGS = {"text_x_tolerance_ratio": 0.6}


def crawl(context: Context) -> None:
    data_path = context.fetch_resource("source.pdf", STATIC_URL)
    for row in h.parse_pdf_table(
        context,
        data_path,
        headers_per_page=True,
        start_page=2,
        page_settings=lambda page: (page, EXTRACT_ARGS),
    ):
        for key, value in row.items():
            row[key] = value.strip().replace("\n", " ")
        bic = row.pop("bic")
        if bic[4:6] == "UT":
            continue
        branch = row.pop("brch_code").strip()
        assert len(branch) == 3, branch
        if branch != "XXX":
            # Skip branches for now:
            # context.log.info(f"Skipping branch: {bic} {branch}")
            continue
        legal_name = row.pop("full_legal_name")
        # Seen as both "130" and "FCC 130" on the internets on similar scraper-y listing websites
        # Map to "130 - FCCCFRP1" if BIC is "FCCCFRP1" because a numeric name is invalid.
        if legal_name == "130" and bic == "FCCCFRP1":
            legal_name = "130 - FCCCFRP1"

        entity = context.make("Organization")
        entity.id = f"bic-{bic}"
        if h.needs_splitting(entity.schema, legal_name):
            h.split_and_apply_names(context, entity, legal_name)
        else:
            entity.add("name", legal_name)
        entity.add("swiftBic", bic)
        entity.add("country", bic[4:6])
        entity.add("address", row.pop("registered_address"))
        entity.add("address", row.pop("operational_address"))
        entity.add("createdAt", row.pop("record_creation_date"))
        entity.add("modifiedAt", row.pop("last_update_date"))
        entity.add("notes", row.pop("branch_description", None))
        type_ = row.pop("instit_type")
        if type_ == "FIIN":
            entity.add("topics", "fin.bank")

        context.audit_data(row, ignore=["branch_address"])
        context.emit(entity)
