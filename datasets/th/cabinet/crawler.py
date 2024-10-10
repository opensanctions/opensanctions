import re
from lxml import html

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

ROLE_PATTERNS = re.compile(
    r"(?P<name>.+?)\s*(?P<role>รองนายกรัฐมนตรี.*|รัฐมนตรีว่าการ.*|รัฐมนตรีประจำ.*|รัฐมนตรีช่วยว่าการ.*|นายกรัฐมนตรี)"
)
# Pattern specifically for "นายกรัฐมนตรี" (Prime Minister)
PRIME_MINISTER_PATTERN = re.compile(r"(?P<role>นายกรัฐมนตรี)\s*(?P<name>.*)")


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
        ".//div[@class='col-lg-1 col-md-1 col-sm-1 col-xs-12']"
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

        # Mtach the text based on "Prime Minister", "Deputy Prime Minister", "Minister" in Thai
        match = ROLE_PATTERNS.search(collected_text)
        if match:
            name = match.group("name").strip()
            role = match.group("role").strip()
        else:
            match = PRIME_MINISTER_PATTERN.search(collected_text)
            if match:
                name = match.group("name").strip()
                role = match.group("role").strip()

        position_summary = doc.find(".//h2[@style='text-align: center;']")
        position_summary = position_summary.text_content().strip()

        person = context.make("Person")
        person.id = context.make_id(name, role)
        person.add("name", name, lang="tha")
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name=role,
            summary=position_summary,
            country="th",
            lang="tha",
            topics=["gov.executive", "gov.national"],
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
