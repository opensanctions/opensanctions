import csv

from zavod import Context, helpers as h
from zavod.logic.pep import categorise

IGNORE = [
    "OrganizationName",
    "PositionStatus",
    "AppointmentTypeDescription",
    "PaymentPlanDescription",
    "LevelGradePay",
    "Tenure",
]


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            agency_name = row.pop("\ufeffAgencyName")
            # org_name = row.pop("OrganizationName")
            position_title = row.pop("PositionTitle", None)
            # appointment_type = row.pop("AppointmentTypeDescription")
            expiration_date = row.pop("ExpirationDate")
            location = row.pop("Location")
            incumbent_first_name = row.pop("IncumbentFirstName")
            incumbent_last_name = row.pop("IncumbentLastName")

            if not incumbent_first_name or not incumbent_last_name:
                continue
            person = context.make("Person")
            person.id = context.make_slug(incumbent_first_name, incumbent_last_name)
            h.apply_name(
                person, first_name=incumbent_first_name, last_name=incumbent_last_name
            )
            person.add("position", f"{position_title}, {agency_name}")  # for dedupe

            position = h.make_position(
                context,
                name=f"{position_title}, {agency_name}",
                subnational_area=location,
            )
            categorisation = categorise(context, position, is_pep=True)
            if categorisation.is_pep:
                occupancy = h.make_occupancy(
                    context, person, position, end_date=expiration_date
                )
                if occupancy:
                    context.emit(person, target=True)
                    context.emit(position)
                    context.emit(occupancy)
            context.audit_data(row, ignore=IGNORE)
