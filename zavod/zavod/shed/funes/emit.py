"""Turn a funes extraction hit into FollowTheMoney entities.

Position names are copied verbatim from pages in unknown languages, so ``lang``
is deliberately omitted: ``make_position`` falls back to the dataset language
for labelling without pretending the source name is in it.
"""

from zavod import helpers as h
from zavod.context import Context
from zavod.entity import Entity
from zavod.shed.funes.definitions import dataset_definition
from zavod.shed.funes.extract import Hit


def emit_hit(context: Context, hit: Hit, *, source_url: str) -> None:
    """Emit the person-position graph of a hit."""
    definition = dataset_definition(context.dataset)
    if definition is None:
        raise ValueError(
            f"Dataset {context.dataset.name} has no config.funes brief; "
            "cannot emit a funes hit"
        )

    for person_data in hit.persons:
        person = context.make("Person")
        person.id = context.make_id(person_data.name, person_data.dob)
        person.add("name", person_data.name)
        h.apply_date(person, "birthDate", person_data.dob)
        person.add("notes", person_data.bio)
        person.add("citizenship", person_data.countries)

        has_occupancy = False
        for position_data in person_data.positions:
            organization: Entity | None = None
            if position_data.organization is not None:
                organization = context.make("Organization")
                organization.id = context.make_id(position_data.organization)
                organization.add("name", position_data.organization)

            position = h.make_position(
                context,
                name=position_data.name,
                description=position_data.description,
                country=position_data.jurisdiction,
                organization=organization,
                source_url=source_url,
            )
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=False,
                start_date=position_data.start_date,
                end_date=position_data.end_date,
            )
            if occupancy is not None:
                context.emit(occupancy)
                context.emit(position)
                if organization is not None:
                    context.emit(organization)
                has_occupancy = True

        if has_occupancy:
            context.emit(person)
