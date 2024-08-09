from zavod import Context, helpers as h

# from zavod.logic.pep import categorise
from popolo_data.importer import Popolo

# popolo = Popolo.from_filename("context.data_url")


def crawl_item(person, context: Context):
    # Create a Person entity
    entity = context.make("Person")
    entity.id = context.make_id(person.id)
    entity.add("name", person.name)
    entity.add("alias", person.other_names)
    entity.add("notes", person.id)
    # entity.add("gender", person.get("gender", "").lower())

    # Emit the person entity
    context.emit(entity)

    # Process memberships for executive committee roles
    position_roles = ["Member of the Executive Committee"]
    for membership in person.memberships:
        role = membership.get("role", "")
        if any(position_role in role for position_role in position_roles):
            occupancy = context.make("Membership")
            occupancy.id = context.make_id(person.id, membership.organization_id, role)
            occupancy.add("person_id", entity.id)
            occupancy.add("role", role)
            occupancy.add("organization_id", membership.organization_id)
            occupancy.add("start_date", membership.get("start_date", ""))
            occupancy.add("end_date", membership.get("end_date", ""))

            # Emit the membership entity
            context.emit(occupancy)


def crawl(context):
    # Load the Popolo data
    response = context.fetch_json(context.data_url)
    if response is None:
        return

    popolo = Popolo(response)

    for person in popolo.persons:
        crawl_item(person, context)

    # Emit positions after processing all persons
    position_roles = ["Member of the Executive Committee"]
    for position_name in position_roles:
        position = context.make("Position")
        position_id = context.make_id(position_name)
        if not position_id:
            context.log.warning(
                f"No ID generated for position {position_name}. Skipping position creation."
            )
            continue
        position.id = position_id
        position.add("name", position_name)

        context.emit(position)
