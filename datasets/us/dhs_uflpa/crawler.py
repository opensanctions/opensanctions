from pprint import pprint
from normality import collapse_spaces, slugify
from zavod import Context
from zavod import helpers as h

import re

FORMATS = ["%B %d, %Y"]

# NAME (and one alias: NAME)
# NAME (including one alias: NAME) and subsidiaries
# NAME (including two aliases: NAME and NAME)
# NAME (including two aliases: NAME; and NAME)            <-- optional oxford comma
# NAME (including three aliases: NAME; NAME; and NAME)
# NAME (including NUMBER aliases: NAME; NAME; ...; and NAME)
# NAME and its subordinate and affiliated entities
# NAME (including three aliases: NAME; NAME; and NAME) and its subordinate and affiliated entities
# NAME and its eight PLACE-based subsidiaries, which include NAME, NAME, ..., and NAME
# NAME and its subsidiary NAME
# NAME, and Subsidiaries

REGEX_NAME_STRUCTURE = re.compile((
    "^"
    "(?P<main>[\w.,/&\(\) -]+?) ?"
    "(\((and|including) [a-z]+ alias(es)? ?: (?P<alias_list>.+)\))? ?"
    "(?P<subordinate_note>, and Subsidiaries| and (subsidiaries|its subordinate and affiliated entities))? ?"
    "(and its [a-zA-Z]+-based subsidiaries, which include (?P<subsidiary_list>.+))?"
    "$"
))
SPLITTERS = [", and ", "; and ", ", ", "; "]

def parse_names(name_field: str):
    print(name_field)
    name_field = name_field.replace(", Ltd.", " Ltd.")
    structure_match = REGEX_NAME_STRUCTURE.match(name_field)
    if structure_match:
        structure = structure_match.groupdict()
        alias_list = structure["alias_list"] or ""
        subsidiary_list = structure["subsidiary_list"] or ""
        names = {
            "main": structure["main"],
            "aliases": h.multi_split(alias_list, SPLITTERS),
            "subsidiaries": h.multi_split(subsidiary_list, SPLITTERS),
            "subordinates_note": structure["subordinate_note"],
        }
        pprint(names)
        #return names
    else:
        print("#################### didn't match")
    print()


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

        parse_names(name)

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
