from zavod import Context, helpers as h


def crawl_item(input_dict: dict, context: Context):
    entity = context.make("Person")

    # The last row of the table is empty
    if all(v is None for v in input_dict.values()):
        return

    id_ = input_dict.pop("id")

    entity.id = context.make_slug(id_)

    entity.add("idNumber", id_)
    entity.add("country", "ps")
    entity.add("topics", "sanction")

    # We are going to split using the dot symbol used to represent the start of a new name
    # Then we will strip leading and trailling spaces
    # Finally, we will remove the information contained in the brackets, because they are not relevant
    names = [
        name.strip()
        for name in input_dict.pop("person_name").split("• ")
        if name.strip()
    ]
    for name in names:
        parts = name.split("(")
        entity.add("name", parts[0].strip())
        aliases = [part.replace(")", "").strip() for part in parts[1:]]
        entity.add("alias", aliases)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", input_dict.pop("date_of_freezing"))
    sanction.add(
        "program",
        "Decree No. (14) of 2015 Concerning the Enforcement of Security Council Resolutions",
    )

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(input_dict)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)
    table = response.find(".//table")
    for row in h.parse_html_table(table, header_tag="td"):
        crawl_item(h.cells_to_str(row), context)
