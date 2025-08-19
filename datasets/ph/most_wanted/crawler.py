import csv
from typing import Dict

from zavod import Context, helpers as h

PDFs = {
    "wanted-with-reward": (
        "https://pnp.gov.ph/wp-content/uploads/2022/10/AsOfOct13_MWPs-with-Reward-criminality_1665621706.pdf",
        "27d929ae510b184bcea75500ff89de759d7a0dd6",
    ),
    "communist-terrorist-group": (
        "https://pnp.gov.ph/wp-content/uploads/2022/10/CTG-with-Reward.pdf",
        "d87d7d22f9e921e94b3049b4b2856283580324b3",
    ),
    "local-terrorist-group": (
        "https://pnp.gov.ph/wp-content/uploads/2022/10/LTG-with-Reward.pdf",
        "3a34d71972f2c67b14a1db66d324f88451851e40",
    ),
}


def parse_name(context, name):
    """
    Always returns: (full_name, first_name, last_name, suffix)
    - Two commas: last, first middle, suffix
    - One comma: last, first middle
    - No comma: everything is full_name, rest blank
    """
    parts = [p.strip() for p in name.split(",")]

    if len(parts) == 3:
        last_name, first_name, suffix = parts
        full_name = None
    elif len(parts) == 2:
        last_name, first_name = parts
        suffix = None
        full_name = None
    else:
        full_name = name.strip()
        first_name = None
        last_name = None
        suffix = None

    return full_name, first_name, last_name, suffix


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    # If no name is provided, skip the row
    if not full_name:
        return
    offense = row.pop("offense")
    case_number = row.pop("case number")
    full_clean, first_name, last_name, suffix = parse_name(context, full_name)

    entity = context.make("Person")
    entity.id = context.make_id(full_name, case_number, offense)
    h.apply_name(
        entity,
        full=full_clean,
        first_name=first_name,
        second_name=last_name,
        suffix=suffix,
    )
    entity.add("alias", row.pop("alias", "").split(";"))
    entity.add("position", row.pop("position", "").split(";"))
    entity.add("topics", "wanted")
    entity.add("country", "ph")
    entity.add("notes", case_number)
    entity.add("sourceUrl", row.pop("source"))

    sanction = h.make_sanction(context, entity, program_name=row.pop("list"))
    h.apply_date(sanction, "listingDate", row.pop("listing date"))
    sanction.add("reason", offense)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["jor-no", "reward"])


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)

    for name, (url, expected_hash) in PDFs.items():
        h.assert_url_hash(context, url, expected_hash)

        pdf_path = context.fetch_resource(name, url)
        h.save_pdf_text_locally(context, name, pdf_path)
