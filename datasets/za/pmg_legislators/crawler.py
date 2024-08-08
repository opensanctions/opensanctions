from zavod import Context, helpers as h
from zavod.logic.pep import categorise


def crawl_item(input_dict: dict, position, categorisation, context: Context):
    entity = context.make("Person")
    entity.id = context.make_id(
        input_dict.get("id", ""),
        input_dict.get("name", {}).get("family_name", ""),
        input_dict.get("name", {}).get("given_name", ""),
    )

    gender = input_dict.get("gender", "").lower()
    entity.add("gender", gender)
    entity.add("email", input_dict.get("email", ""))
    entity.add("address", input_dict.get("address", {}).get("label", ""))
    # Extract phone numbers
    phone_details = input_dict.get("contact_details", [])
    for contact in phone_details:
        if contact.get("type") == "phone":
            entity.add("phone", contact.get("value"))
        elif contact.get("type") == "website":
            entity.add("website", contact.get("value"))

    entity.add("website", input_dict.get("contact_details", {}).get("website", ""))
    # Process memberships
    memberships = input_dict.get("memberships", [])
    for membership in memberships:
        role = membership.get("role", "")
        organization = membership.get("organization_id", "")
        occupancy = h.make_occupancy(
            context,
            entity,
            position if organization in {position.prefix} else role,
            True,
            categorisation=categorisation,
        )
        occupancy.add("role", role)
        occupancy.add("organization_id", organization)
        context.emit(occupancy)

    context.emit(entity, target=True)


def crawl(context: Context):
    """
    Entrypoint to the crawler.
    The crawler works by fetching the data from the URL as JSON.
    Finally, we create the entities.
    :param context: The context object.
    """
    data_url = "https://pa.org.za/media_root/popolo_json/pombola.json"
    response = context.fetch_json(data_url)
    if response is None:
        return

    positions = [
        ("Member of the National Assembly", "national-assembly"),
        (
            "Member of the National Council of Provinces",
            "national-council-of-provinces",
        ),
        ("Minister", "executive"),
        ("Member of the Provincial Legislature", "provincial-legislature"),
        ("Member of the Executive Committee", "executive-committee"),
    ]

    for position_name, position_id in positions:
        position = h.make_position(context, position_name, country="za")
        # categorisation = categorise(context, position, is_pep=True)
        context.emit(position)

        for person in response.get("persons", []):
            for membership in person.get("memberships", []):
                if membership.get("organization_id") == position_id:
                    crawl_item(person, position_name, categorisation, context)
