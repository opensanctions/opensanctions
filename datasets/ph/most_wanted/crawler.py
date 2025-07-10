import csv
from typing import Dict

from zavod import Context, helpers as h

# from rigour.mime.types import PDF
# from pathlib import Path
# from zavod.shed.gpt import run_image_prompt

# prompt = """
# Extract structured data from the following page of a PDF document.

# Return a JSON list (`mvps`). For each person entry, return a JSON object with these fields:
# - `name`
# - `alias` (as a list, splitting at semicolons)
# - `offenses`
# - `issuing_court`
# - `criminal_case_numbers`
# - `reward`
# - `dilg_mc_no`
# - `region`

# If any field is missing or not specified, return an empty string or an empty list as appropriate.
# Be as faithful to the original text as possible—do not add or infer information.
# For example, if a reward is listed as `1,200,000.00`, use this exact value.
# Preserve line breaks and punctuation from the original where possible.

# Return your answer as a single JSON object.
# """


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
    # A way to update the data in the future in case the source changes
    #
    # path = Path(__file__).parent / "most_wanted_persons.pdf"
    # for page_path in h.make_pdf_page_images(path):
    #     data = run_image_prompt(context, prompt, page_path)
    #     assert "mvps" in data, data
    #     for holder in data.get("mvps", []):
    #         pprint(holder)

    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
