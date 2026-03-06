from zavod import Context, helpers as h

PROGRAM_KEY = "GB-RUS"
PROGRAM_NAME = "Russia"


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    table = h.xpath_element(doc, ".//table")
    for row in h.parse_html_table(table):
        str_row = h.cells_to_str(row)
        org_name = str_row.pop("organisation")
        historic_id = str_row.pop("historic_group_id_1")
        link_el = row.pop(
            "subject_to_other_sanctions_see_entry_on_the_uk_sanctions_list"
        )
        url_list = h.xpath_strings(link_el, ".//a/@href")

        entity = context.make("LegalEntity")
        # We preserve historic id for backwards compatibility
        entity.id = context.make_slug(historic_id)
        entity.add("name", org_name)
        if url_list:
            entity.add("sourceUrl", url_list[0])

        sanction = h.make_sanction(
            context,
            entity,
            program_name=PROGRAM_NAME,
            program_key=PROGRAM_KEY,
        )

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(
            str_row,
            [
                "uk_sanctions_list_unique_id_2",
                "subject_to_other_sanctions_see_entry_on_the_uk_sanctions_list",
            ],
        )
