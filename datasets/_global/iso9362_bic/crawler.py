from normality import slugify
from typing import List, Optional
import pdfplumber

from zavod import Context

EXTRACT_ARGS = {"text_x_tolerance_ratio": 0.6}


def crawl(context: Context) -> None:
    data_path = context.fetch_resource("source.pdf", context.data_url)
    pdf = pdfplumber.open(data_path.as_posix())
    for idx, page in enumerate(pdf.pages):
        if idx == 0:
            page.close()
            continue
        if idx % 100 == 0:
            context.log.info(f"Processing page {idx}...")
        headers: Optional[List[str]] = None
        for row in page.extract_table(EXTRACT_ARGS):
            row = [cell.strip().replace("\n", " ") for cell in row]
            if headers is None:
                headers = [slugify(c, "_") for c in row]
                continue
            row = dict(zip(headers, row))
            bic = row.pop("bic")
            if bic[4:6] == "UT":
                continue
            branch = row.pop("brch_code").strip()
            assert len(branch) == 3
            if branch != "XXX":
                # Skip branches for now:
                # context.log.info(f"Skipping branch: {bic} {branch}")
                continue
            entity = context.make("Organization")
            entity.id = f"bic-{bic}"
            entity.add("name", row.pop("full_legal_name"))
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
        page.close()
    pdf.close()
