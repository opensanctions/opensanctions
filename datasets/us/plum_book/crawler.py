import csv
import html

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

IGNORE = [
    "Organization",
    "Position Status",
    "Pay Plan",
    "Level, Grade, or Pay",
    "Tenure",
    "Individual Unique ID",
]


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            agency_name = row.pop("Agency")
            position_title = row.pop("Position Title")
            appointment_type = row.pop("Appointment Type")
            start_date = row.pop("Begin Date")
            # 'Expiration date (for term and time-limited appointments) is the date
            #  when the person has to vacate the position
            # 'Vacate Date' is the date when the person vacated the position
            end_date = row.pop("Vacate Date", row.pop("Expiration Date"))
            location = row.pop("Duty Location")
            incumbent_first_name = row.pop("First Name")
            incumbent_last_name = row.pop("Last Name")
            position_name = f"{position_title}, {agency_name}"

            if not incumbent_first_name or not incumbent_last_name:
                continue
            person = context.make("Person")
            person.id = context.make_id(
                incumbent_first_name, incumbent_last_name, position_name
            )
            person.add("country", "us")  # although most are US citizens in practice

            # Cleaning after make_id
            incumbent_first_name = html.unescape(incumbent_first_name)
            incumbent_last_name = html.unescape(incumbent_last_name)
            position_name = html.unescape(position_name)

            h.apply_name(
                person, first_name=incumbent_first_name, last_name=incumbent_last_name
            )
            person.add("position", position_name)  # for dedupe

            position = h.make_position(
                context,
                name=position_name,
                subnational_area=location,
                country="us",
            )

            categorisation = categorise(context, position, default_is_pep=True)
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
