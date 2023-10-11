from zavod import Context
from zavod import helpers as h


def crawl_program(context: Context, table, program: str) -> None:
    print(program)
    


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.data.url, cache_days=7)
    tables = doc.findall('.//table[@class="usa-table"]')
    for table in tables:
        program_container = table.getprevious()
        description = program_container.find(".//strong")
        section_link = program_container.find('.//a')
        print(program_container, description, section_link)

        if description is not None and section_link is not None:
            program = program_container.text_content()
            crawl_program(context, table, program)
        else:
            context.log.warning("Couldn't get program text for table.")