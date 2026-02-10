import csv
import pdfplumber
import re

from normality import squash_spaces
from pathlib import Path

from zavod import Context


def save_to_csv(data: list[dict], filename: str = "names.csv"):
    """Save to CSV with name, company, raw_input columns."""
    filepath = Path(filename)

    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "company", "raw_input"])
        writer.writeheader()
        writer.writerows(data)


def extract_names_simple(raw_text: str, company_name: str) -> list[dict]:
    """Extract all names by splitting on separators."""
    # Remove everything after "Arvode"
    text = raw_text.split("Arvode")[0]

    # Remove the prefixes and metadata
    text = text.replace("Ordf:", ",")
    text = text.replace("Vd:", ",")
    text = text.replace("Led:", ",")
    text = text.replace("Arb rep:", ",")
    text = text.replace("Rev:", ",")
    text = text.replace("Statens ägarandel:", ",")
    text = text.replace("Styrelse och revisorer valda för", ",")
    text = text.replace("och", ",")
    text = text.replace("\n", ",")

    text = re.sub(r"\([^)]+\)", "", text)

    # Split on commas and dots
    text = text.replace(".", ",")
    parts = text.split(",")

    results = []
    for part in parts:
        part = part.strip()
        # Skip if empty, too short, contains numbers, or contains %
        if not part or len(part) < 3 or any(c.isdigit() for c in part) or "%" in part:
            continue

        # Basic check: should have at least one space (first name + last name)
        if " " in part:
            results.append(
                {"name": part, "company": company_name, "raw_input": raw_text}
            )

    return results


def crawl_entity(context: Context, page, width) -> str:
    # Extract left 65% of page, starting below header
    # This is where the company description is
    # bbox = (0, 110, width * 0.65, 300)
    bbox = (0, 110, width * 0.65, 150)
    cropped = page.within_bbox(bbox)
    text = cropped.extract_text()

    # Get first 50 characters
    first_chars = text[:50].strip()
    # context.log.info(first_sentence=first_sentence)
    res = context.lookup(
        "company_names", squash_spaces(first_chars), warn_unmatched=True
    )
    entity_name = None
    if res and res.name:
        entity_name = res.name
    assert entity_name

    entity = context.make("LegalEntity")
    entity.id = context.make_id(entity_name, first_chars)
    assert entity.id
    entity.add("name", entity_name)
    context.emit(entity)

    return entity_name


def extract_data(context, page, width, entity_name):
    # Crop from the right and skip the header
    # x: 65% to 100% of width
    # y: 110 to 260 (skip header "Bolag med statligt ägande A–Ö")
    bbox = (width * 0.65, 110, width, 260)

    cropped = page.within_bbox(bbox)
    text = cropped.extract_text()
    return extract_names_simple(text, entity_name)


def crawl(context: Context):
    pdf_path = context.fetch_resource("source.pdf", context.data_url)
    all_data = []  # Collect all data here

    with pdfplumber.open(pdf_path) as pdf:
        for page_num in range(34, 75):
            if page_num in [72, 46, 59]:
                context.log.info("Skipping page")
                continue
            page = pdf.pages[page_num]
            width = page.width

            entity_name = crawl_entity(context, page, width)
            data = extract_data(context, page, width, entity_name)
            all_data.extend(data)

    # Write everything at once
    save_to_csv(all_data, "board1.csv")
