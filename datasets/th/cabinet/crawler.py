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

    # For lack of anything more semantic, we select persons based on sizing of their containers
    prime_minister_containers = main_container.xpath(
        ".//div[contains(@style, 'min-height: 480; max-height: 600')]"
    )
    assert len(prime_minister_containers) == 1, prime_minister_containers
    prime_minister = prime_minister_containers[0]
    persons = main_container.findall(
        ".//div[@style='min-height: 360; max-height: 500;']"
    )
    persons.append(prime_minister)

    for person in persons:
        # Ensure there's whitespace between headings
        for heading in person.xpath(
            ".//*[self::h3 or self::h4 or self::h5 or self::h6]"
        ):
            heading.tail = heading.tail + "\n" if heading.tail else "\n"

        collected_text = person.text_content().strip()
        if not collected_text:
            continue

        # Match the text based on "Prime Minister", "Deputy Prime Minister", "Minister" in Thai
        match = ROLE_PATTERNS.search(collected_text)
        if match:
            name = match.group("name").strip()
            role = match.group("role").strip()
        else:
            match = PRIME_MINISTER_PATTERN.search(collected_text)
            if match:
                name = match.group("name").strip()
                role = match.group("role").strip()
            else:
                context.log.warning("Could not match name and role", collected_text)
                continue

        person = context.make("Person")
        person.id = context.make_id(name, role)
        person.add("name", name, lang="tha")
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name=role,
            country="th",
            lang="tha",
            topics=["gov.executive", "gov.national"],
        )

        categorisation = categorise(context, position, is_pep=True)
        if categorisation.is_pep:
            occupancy = h.make_occupancy(context, person, position)
            if occupancy:
                context.emit(person, target=True)
                context.emit(position)
                context.emit(occupancy)

        # Find the date of the last update
    last_upd = doc.find(".//h5[@style='text-align: center;']")
    date_last_upd = last_upd.text_content().strip()
    assert date_last_upd == "ปรับปรุงข้อมูล ณ วันที่ 4 กันยายน 2567"  # September 4, 2024
