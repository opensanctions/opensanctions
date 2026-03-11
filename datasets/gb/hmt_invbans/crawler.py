from zavod import Context, helpers as h

PROGRAM_KEY = "GB-RUS-INV"


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    table = h.xpath_element(doc, ".//table")
    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        org_name = str_row.pop("organisation")
        historic_id = str_row.pop("historic_group_id_1")
        # Refers to entries that are also included on the UK Sanctions List.
        current_id = str_row.pop("uk_sanctions_list_unique_id_2")
        # Prefer UK Sanctions List ID; fall back to historic OFSI group ID.
        entity_id = current_id if current_id != "-" else historic_id

        entity = context.make("LegalEntity")
        entity.id = context.make_slug(entity_id)
        entity.add("name", org_name)
        # URL is not there for entities that are not on the UK Sanctions List.
        url_list = h.xpath_strings(
            row.pop("subject_to_other_sanctions_see_entry_on_the_uk_sanctions_list"),
            ".//a/@href",
        )
        if url_list:
            entity.add("sourceUrl", url_list[0])

        sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(
            str_row,
            ["subject_to_other_sanctions_see_entry_on_the_uk_sanctions_list"],
        )
