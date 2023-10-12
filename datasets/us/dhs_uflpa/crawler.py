from normality import collapse_spaces, slugify
from zavod import Context
from zavod import helpers as h

FORMATS = ["%B %d, %Y"]

def crawl_program(context: Context, table, program: str, section: str) -> None:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(el.text_content()) for el in row.findall("./th")]
            continue

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        data = {hdr: c for hdr, c in zip(headers, cells)}

        entity = context.make("Company")
        name = data.pop("name-of-entity", data.pop("entity-name", None))
        if name is None:
            context.log.warning("Couldn't get entity name", data)
            continue
        entity.id = context.make_id(name, "md")
        entity.add("name", name)
        entity.add("topics", "sanction")

        sanction = h.make_sanction(context, entity, section)
        sanction.add("program", program)
        sanction.add("startDate", h.parse_date(data.pop("effective-date"), FORMATS))

        context.emit(entity, target=True)
        context.emit(sanction)

        context.audit_data(data)


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.data.url, cache_days=7)
    tables = doc.findall('.//table[@class="usa-table"]')
    for table in tables:
        program_container = table.getprevious()
        description = program_container.find(".//strong")
        section_link = program_container.find(".//a")

        if description is not None and section_link is not None:
            section = section_link.text_content()
            program = f"{description.text_content()} - {section}"
            crawl_program(context, table, program, section)
        else:
            context.log.warning("Couldn't get program text for table.")
