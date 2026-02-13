import csv

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus


SOE_REPORT_URL = "https://www.regeringen.se/contentassets/a2be3c80b3384f3eadc64530f6a2ff23/verksamhetsberattelse--for-bolag-med-statligt-agande-2024.pdf"


def crawl(context: Context) -> None:
    # One-time CSV extraction from PDF (2025-02-11)
    # Used once to extract board members from the source PDF into se_soe.csv
    # Now commented out as we read from the published Google Sheet instead
    # If the PDF format changes significantly in next year's update, may need to re-enable

    # csv_from_pdf(context, "se_soe.csv")

    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            company_name = row.pop("company")
            pep_name = row.pop("name")
            position_name = row.pop("position")

            # Skip auditors and representatives
            if "Auditor" in position_name or "Employee Representative" in position_name:
                continue

            company = context.make("Company")
            company.id = context.make_id(company_name)
            company.add("name", company_name)

            pep = context.make("Person")
            pep.id = context.make_id(pep_name)
            pep.add("name", pep_name)
            # Using 'country' rather than 'citizenship' as board members may be non-Swedish citizens
            pep.add("country", "se")
            pep.add("sourceUrl", row.pop("source_url"))

            position = h.make_position(
                context,
                name=f"{position_name}, {company_name}",
                topics=["gov.soe"],
                country="se",
                organization=company,
            )
            categorisation = categorise(context, position, is_pep=True)

            occupancy = h.make_occupancy(
                context,
                pep,
                position,
                False,
                categorisation=categorisation,
                status=OccupancyStatus.UNKNOWN,
            )

            if occupancy is not None:
                context.emit(pep)
                context.emit(company)
                context.emit(position)
                context.emit(occupancy)


# import pdfplumber
# from normality import squash_spaces
# from pathlib import Path

# # Define position markers in order of appearance
# POSITION_MARKERS = ["Ordf:", "Vd:", "Led:", "Arb rep:", "Rev:"]
# POSITION_NAMES = {
#     "Ordf:": "Chair",
#     "Vd:": "CEO",
#     "Led:": "Board Member",
#     "Arb rep:": "Employee Representative",
#     "Rev:": "Auditor",
# }


# def csv_from_pdf(context, filename: str):
#     pdf_path = context.fetch_resource("source.pdf", SOE_REPORT_URL)
#     soe_leadrship = []
#     with pdfplumber.open(pdf_path) as pdf:
#         for page_num in range(34, 75):
#             if page_num in [72, 46, 59]:
#                 context.log.info("Skipping empty page")
#                 continue
#             page = pdf.pages[page_num]
#             width = page.width

#             entity_name = get_entity_name(context, page, width)
#             peps = extract_board_member(context, page, width, entity_name)
#             soe_leadrship.extend(peps)
#     # Write everything at once
#     filepath = Path(__file__).parent / filename
#     with open(filepath, "w", encoding="utf-8", newline="") as f:
#         writer = csv.DictWriter(
#             f, fieldnames=["name", "position", "company", "raw_input", "source_url"]
#         )
#         writer.writeheader()
#         writer.writerows(soe_leadrship)


# def extract_board_member(
#     context: Context, page, width: float, company_name: str
# ) -> list[dict]:
#     """Extract board member names with positions from right column of page."""
#     # Crop from the right and skip the header
#     # x: 65% to 100% of width
#     # y: 110 to 260 (skip header "Bolag med statligt ägande A–Ö")
#     bbox = (width * 0.65, 110, width, 260)
#     cropped = page.within_bbox(bbox)

#     raw_text = cropped.extract_text()
#     # Remove everything after "Arvode" if present
#     text = raw_text
#     if "Arvode" in text:
#         text = text.split("Arvode")[0]

#     # Find positions of each marker in the text
#     marker_positions = []
#     for marker in POSITION_MARKERS:
#         if marker in text:
#             idx = text.find(marker)
#             marker_positions.append((idx, marker))

#     # Sort by position in text
#     marker_positions.sort()

#     # Extract names for each position
#     results = []
#     for i, (start_idx, marker) in enumerate(marker_positions):
#         # Find where this section ends (next marker or end of text)
#         if i + 1 < len(marker_positions):
#             end_idx = marker_positions[i + 1][0]
#             section_text = text[start_idx:end_idx]
#         else:
#             section_text = text[start_idx:]

#         # Remove the marker itself
#         section_text = section_text.replace(marker, "")
#         # Extract names from this section
#         names = clean_names(section_text)
#         # Add to results with position info
#         position = POSITION_NAMES.get(marker, marker)
#         for name in names:
#             results.append(
#                 {
#                     "name": name,
#                     "position": position,
#                     "company": company_name,
#                     "raw_input": raw_text,
#                     "source_url": SOE_REPORT_URL,
#                 }
#             )

#     return results


# def clean_names(section_text: str) -> list[str]:
#     """Extract individual names from a section of text."""
#     # Replace common separators with commas
#     text = section_text
#     text = text.replace(" och ", ",")
#     text = text.replace("\n", ",")
#     text = text.replace(".", ",")
#     # Remove parenthetical content (e.g., company names in parentheses)
#     while "(" in text and ")" in text:
#         start = text.find("(")
#         end = text.find(")", start)
#         if end > start:
#             text = text[:start] + text[end + 1 :]
#         else:
#             break
#     # Split on commas
#     parts = text.split(",")
#     names = []
#     for part in parts:
#         part = part.strip()
#         # Skip if empty, too short, contains numbers, or contains %
#         if not part or len(part) < 3:
#             continue
#         if any(c.isdigit() for c in part):
#             continue
#         if "%" in part:
#             continue
#         # Should have at least one space (first name + last name)
#         if " " in part:
#             names.append(part)
#     return names


# def get_entity_name(context: Context, page, width) -> str:
#     # Extract left 65% of page, starting below header
#     # This is where the company description is
#     # bbox = (0, 110, width * 0.65, 300)
#     bbox = (0, 110, width * 0.65, 150)
#     cropped = page.within_bbox(bbox)
#     text = cropped.extract_text()
#     # Get first 50 characters
#     first_chars = text[:50].strip()
#     # context.log.info(first_sentence=first_sentence)
#     res = context.lookup(
#         "company_names", squash_spaces(first_chars), warn_unmatched=True
#     )
#     entity_name = None
#     if res and res.name:
#         entity_name = res.name
#     assert entity_name
#     return entity_name
