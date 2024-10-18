# import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

# TITLE_REGEX = re.compile(r"^(Mr|Ms|Miss|Prof|Dr)\.? (?P<name>.+)$")


def unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='directory-list']") is not None

def board_unblock_validator(doc) -> bool:
    return doc.find(".//div[@class='name']") is not None

def crawl(context: Context):
    doc = fetch_html(
        context, context.data_url, unblock_validator, html_source="httpResponseBody", cache_days=3
    )
    statutory_boards = doc.findall(".//div[@class='directory-list']//ul[@class='ministries']//li//a")
    boards_dict = {}
    for board in statutory_boards:
        link = board.get("href")
        name = board.text_content().strip()
        if link is None or name == "":
            context.log.error(f"No link or name found for {name}")
        boards_dict[name] = link

    for name, link in boards_dict.items():
        print(f"Fetching data for {name}: {link}")
        if link is not None: 
            board_doc = fetch_html(
            context, link, board_unblock_validator, html_source="httpResponseBody", cache_days=3
        )
            officials = board_doc.findall(".//li[@id]")
            for official in officials:
                #print(person.text_content())
                position = official.find(".//div[@class='rank']").text_content().strip()
                full_name = official.find(".//div[@class='name']").text_content().strip()
                role = official.find(".//div[@class='detail']").text_content().strip()
                email = official.find(".//div[@class='email info-contact']").text_content().strip()
                # data also contains phone numbrs in some cases
                print(f"Position: {position}, Name: {full_name}, Role: {role}, Email: {email}")
                person = context.make("Person")
                person.id = context.make_id(full_name, position)
                person.add("name", full_name)
                if role is not None:
                    person.add("position", role)
                person.add("topics", "role.pep")
                if link is not None:
                    person.add("sourceUrl", link)

                position = h.make_position(
                    context,
                    name="position name", # adjust the logic dependign on the institution
                    country="sg",
                    topics=["gov.executive", "gov.national"],
                )

                categorisation = categorise(context, position, is_pep=True)
                if categorisation.is_pep:
                    occupancy = h.make_occupancy(context, person, position)
                    if occupancy:
                        context.emit(person, target=True)
                        context.emit(position)
                        context.emit(occupancy)