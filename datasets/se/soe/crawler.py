import csv

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus

# from extract_csv_from_report_pdf_2024 import csv_from_pdf


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
