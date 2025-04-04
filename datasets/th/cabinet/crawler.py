import re
from normality import collapse_spaces

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.shed.trans import apply_translit_full_name, make_position_translation_prompt
from zavod.shed.zyte_api import fetch_html

ROLE_PATTERNS = re.compile(
    r"(?P<name>.+?)\s*(?P<role>รองนายกรัฐมนตรี.*|รัฐมนตรีว่าการ.*|รัฐมนตรีประจำ.*|รัฐมนตรีช่วยว่าการ.*|นายกรัฐมนตรี)"
)
# Pattern specifically for "นายกรัฐมนตรี" (Prime Minister)
PRIME_MINISTER_PATTERN = re.compile(r"(?P<role>นายกรัฐมนตรี)\s*(?P<name>.*)")
REGEX_TITLES = re.compile(r"^(นางสาว|นาง|นาย|พลตำรวจเอก|พันตำรวจเอก|พลเอก)")
POSITION_PROMPT = prompt = make_position_translation_prompt("tha")
TRANSLIT_OUTPUT = {"eng": ("Latin", "English")}


def crawl(context: Context):
    prime_min_xpath = ".//div[contains(@style, 'min-height: 480; max-height: 600')]"
    doc = fetch_html(context, context.data_url, prime_min_xpath)
    main_container = doc.find(".//div[@class='wonderplugintabs-panel-inner']")

    # For lack of anything more semantic, we select persons based on sizing of their containers
    prime_minister_containers = main_container.xpath(prime_min_xpath)
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

        collected_text = collapse_spaces(person.text_content())
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
        title_match = REGEX_TITLES.match(name)
        if not title_match:
            context.log.warning("Could not match title in name.", name=name)
            continue
        name = REGEX_TITLES.sub("", name)
        person.add("name", name, lang="tha")
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name=role,
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
