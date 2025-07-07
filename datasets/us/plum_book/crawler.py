import csv

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

IGNORE = [
    "OrganizationName",
    "PositionStatus",
    "PaymentPlanDescription",
    "LevelGradePay",
    "Tenure",
]


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            agency_name = row.pop("AgencyName")
            position_title = row.pop("PositionTitle")
            appointment_type = row.pop("AppointmentTypeDescription")
            # 'Expiration date (for term and time-limited appointments)'
            # The assumption here is that it's the date when the position expires
            expiration_date = row.pop("ExpirationDate")
            # 'IncumbentBeginDate' is the date when the person started
            start_date = row.pop("IncumbentBeginDate")
            # 'IncumbentVacateDate' is the date when the person vacated the position
            end_date = row.pop("IncumbentVacateDate")
            location = row.pop("Location")
            incumbent_first_name = row.pop("IncumbentFirstName")
            incumbent_last_name = row.pop("IncumbentLastName")
            position_name = f"{position_title}, {agency_name}"

            if not incumbent_first_name or not incumbent_last_name:
                continue
            person = context.make("Person")
            person.id = context.make_id(
                incumbent_first_name, incumbent_last_name, position_name
            )
            h.apply_name(
                person, first_name=incumbent_first_name, last_name=incumbent_last_name
            )
            person.add("position", position_name)  # for dedupe

            position = h.make_position(
                context,
                name=position_name,
                subnational_area=location,
                country="us",
                dissolution_date=expiration_date,
            )

            categorisation = categorise(context, position, is_pep=True)
            if not categorisation.is_pep:
                continue
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                start_date=start_date,
                end_date=end_date,
            )
            if occupancy is not None:
                occupancy.add("description", appointment_type)
            if not occupancy:
                continue
            context.emit(person)
            context.emit(position)
            context.emit(occupancy)
            context.audit_data(row, ignore=IGNORE)
