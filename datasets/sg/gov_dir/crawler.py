import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

TITLE_REGEX = re.compile(r"^(Mr|Ms|Miss|Prof|Dr)\.? (?P<name>.+)$")


def unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='directory-list']") is not None

def board_unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='name']") is not None

# def emit_person(context: Context, name, role, link, title, position_name):
#     person = context.make("Person")
#     person.id = context.make_id(name, role)
#     person.add("name", name)
#     person.add("title", title)
#     person.add("position", role)
#     person.add("topics", "role.pep")
#     if link is not None:
#         person.add("sourceUrl", link)

#     position = h.make_position(
#         context,
#         name=position_name,
#         country="sg",
#         topics=["gov.executive", "gov.national"],
#     )

#     categorisation = categorise(context, position, is_pep=True)
#     if categorisation.is_pep:
#         occupancy = h.make_occupancy(context, person, position)
#         if occupancy:
#             context.emit(person, target=True)
#             context.emit(position)
#             context.emit(occupancy)



def crawl(context: Context):
    doc = fetch_html(
        context, context.data_url, unblock_validator, html_source="httpResponseBody"
    )
    statutory_boards = doc.findall(".//div[@class='directory-list']//ul[@class='ministries']//li//a")
    boards_dict = {}
    for board in statutory_boards:
        link = board.get("href")
        name = board.text_content().strip()
        if link or name is None: 
            context.log.error("No link or name found for board")
        boards_dict[name] = link

    for name, link in boards_dict.items():
        print(f"Fetching data for {name}: {link}")
        if link is not None: 
            board_doc = fetch_html(
            context, link, board_unblock_validator, html_source="httpResponseBody"
        )
            person = board_doc.findall(".//li[@id]")
            for p in person:
                print(p.text_content())
        # Optionally print the fetched content or handle as per requirement
            print(f"Content fetched for {name}")
 
    