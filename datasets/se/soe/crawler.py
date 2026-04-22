import csv

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus

# from extract_csv_from_report_pdf_2024 import csv_from_pdf


def check_updates(context: Context) -> None:
    """
    Verify that the source report URL hasn't changed.

    If this assertion fails, it likely means a new annual report has been published.
    You'll need to:
    1. Update SOE_REPORT_URL to point to the new report PDF in extract_csv_from_report_pdf_2024.py
    2. Update the expected URL in this assertion
    3. Review and update the 'company_names' lookup table for any new/changed companies
    4. Manually verify the extracted data after re-running
    """
    assert context.dataset.url is not None
    doc = context.fetch_html(context.dataset.url, absolute_links=True)
    report_2024_url = h.xpath_string(
        doc,
        "//section[contains(@class, 'has-blockdivider')]//a[contains(text(), 'Verksamhetsberättelse för bolag med statligt ägande')]/@href",
    )
    assert (
        report_2024_url
        == "https://www.regeringen.se/rapporter/2025/06/verksamhetsberattelse-for-bolag-med-statligt-agande-2024/"
    )


def crawl(context: Context) -> None:
    # One-time CSV extraction from PDF (2025-02-11)
    # Used once to extract board members from the source PDF into se_soe.csv
    # Now commented out as we read from the published Google Sheet instead
    # If the PDF format changes significantly in next year's update, may need to re-enable

    # csv_from_pdf(context, "se_soe.csv")
    check_updates(context)

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
