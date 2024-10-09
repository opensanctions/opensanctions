import re
from lxml import html

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html
from zavod.shed.trans import (
    apply_translit_full_name,
    make_position_translation_prompt,
)

TRANSLIT_OUTPUT = {"eng": ("Latin", "English")}
POSITION_PROMPT = prompt = make_position_translation_prompt("tha")
ROLE_PATTERNS = re.compile(
    r"(?P<name>.+?)\s*(?P<role>รองนายกรัฐมนตรี.*|รัฐมนตรีว่าการ.*|รัฐมนตรีประจำ.*|รัฐมนตรีช่วยว่าการ.*|นายกรัฐมนตรี)"
)


def unblock_validator(doc: html.HtmlElement) -> bool:
    return (
        len(
            doc.xpath(
                ".//div[contains(@class, 'col-lg-2 col-md-2 col-sm-6 col-xs-12')]"
            )
        )
        > 0
    )


def crawl(context: Context):
    doc = fetch_html(
        context, context.data_url, unblock_validator=unblock_validator, cache_days=3
    )
    main_container = doc.find(".//div[@class='wonderplugintabs-panel-inner']")

    prime_minister = main_container.find(
        ".//div[@style='min-height: 480; max-height: 600; display: flex; align-items: center; flex-direction: column; justify-content: center; width: 100%;']"  # to be updated
    )
    persons = main_container.findall(
        ".//div[@style='min-height: 360; max-height: 500;']"
    )
    persons.append(prime_minister)

    h_tags = [".//h6", ".//h5", ".//h3", ".//h4"]  # order of iteration matters
    for person in persons:
        collected_texts = []
        for h_tag in h_tags:
            name_fields = person.findall(h_tag)
            for field in name_fields:
                text_content = field.text_content().strip()
                if text_content:
                    collected_texts.append(text_content)

        # Join all collected text with a space
        collected_text = " ".join(collected_texts)

        # Split the text based on "Prime Minister", "Deputy Prime Minister", "Minister" in Thai
        match = ROLE_PATTERNS.search(collected_text)
        if match:
            name = match.group("name").strip()
            role = match.group("role").strip()
            print(f"Name: {name}, Role: {role}")
        else:
            # If no role can be matched, print the entire text
            print(f"Name: {collected_text}, Role: None")

        position_summary = doc.find(".//h2[@style='text-align: center;']")
        position_summary = position_summary.text_content().strip()

        person = context.make("Person")
        person.id = context.make_id(name, role)
        person.add("name", name, lang="tha")
        apply_translit_full_name(context, person, "tha", name, TRANSLIT_OUTPUT)
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name=role,
            summary=position_summary,
            country="th",
            lang="tha",
            topics=["gov.executive", "gov.national"],
        )
        apply_translit_full_name(
            context, position, "tha", role, TRANSLIT_OUTPUT, POSITION_PROMPT
        )

        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(context, person, position)
            if occupancy:
                context.emit(person)
                context.emit(position)
                context.emit(occupancy)

        # Find the date of the last update
    last_upd = doc.find(".//h5[@style='text-align: center;']")
    date_last_upd = last_upd.text_content().strip()
    assert date_last_upd == "ปรับปรุงข้อมูล ณ วันที่ 4 กันยายน 2567"  # September 4, 2024
