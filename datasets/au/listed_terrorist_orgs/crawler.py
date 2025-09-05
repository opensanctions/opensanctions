from typing import Optional

from zavod import Context
from zavod import helpers as h


AU_TERROR = "AU-TERROR"


def parse_listing_dates(date_text: Optional[str]) -> Optional[str]:
    """Parse the listing date text to extract the initial listing date."""
    if not date_text:
        return None

    # Split by common delimiters and get the first date
    # Format appears to be: "Initial date, Re-listed: date1, Re-listed: date2"
    parts = date_text.split(",")
    if parts:
        first_date = parts[0].strip()
        # Remove any "Re-listed:" prefix if present
        first_date = first_date.replace("Re-listed:", "").strip()
        return first_date

    return date_text.strip()


def crawl(context: Context) -> None:
    """Crawl Australian listed terrorist organizations."""
    doc = context.fetch_html(context.data_url)

    # Find the table containing terrorist organizations
    table = doc.find(".//table")
    if table is None:
        context.log.error("Could not find terrorist organizations table")
        return

    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            # Extract headers from the first row
            header_cells = row.findall("./th") or row.findall("./td")
            if header_cells:
                headers = [h.element_text(cell) for cell in header_cells]
                context.log.info("Found headers", headers=headers)
                continue

        # Process data rows
        cells = row.findall("./td")
        if len(cells) < 2:
            continue

        org_name = h.element_text(cells[0])
        listing_date_text = h.element_text(cells[1])

        if not org_name:
            context.log.warning("Empty organization name")
            continue

        # Create organization entity
        organization = context.make("Organization")
        organization.id = context.make_id(org_name)
        organization.add("name", org_name)
        organization.add("topics", "crime.terror")

        # Parse and add listing date
        listing_date = parse_listing_dates(listing_date_text)

        # Create sanction entity
        sanction = h.make_sanction(
            context,
            organization,
            key="au-terrorist-listing",
            program_key=h.lookup_sanction_program_key(context, AU_TERROR),
        )

        # Add listing date to sanction
        if listing_date:
            h.apply_date(sanction, "startDate", listing_date)

        # Add additional sanction details
        sanction.set(
            "reason", "Listed as terrorist organisation under Criminal Code Act 1995"
        )
        sanction.set("authority", "Attorney-General of Australia")

        context.emit(organization)
        context.emit(sanction)

        context.log.info("Processed organization", name=org_name, date=listing_date)
