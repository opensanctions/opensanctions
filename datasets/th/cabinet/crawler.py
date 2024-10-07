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
        ".//div[@style='min-height: 400; max-height: 600;']"  # to be updated
    )
    persons = main_container.findall(
        ".//div[@style='min-height: 360; max-height: 500;']"
    )
    persons.append(prime_minister)
    for person in persons:
        name_field = person.find(".//h4[1]")
        if name_field is None or not name_field.text_content().strip():
            name_field = person.find(".//h5[1]")
            if name_field is None:
                name_field = person.find(".//h6[1]")

        name = name_field.text_content().strip() if name_field is not None else None
        if not name:
            context.log.warning(f"Name missing for {person}")

        # Extract the text from the second <h4>, falling back to <h5> and <h6> if necessary
        role_field = person.find(".//h4[2]")
        if role_field is None or not role_field.text_content().strip():
            role_field = person.find(".//h5[2]")
            if role_field is None:
                role_field = person.find(".//h6[2]")

        role = role_field.text_content().strip() if role_field is not None else None
        if not role:
            context.log.warning(f"Position missing for {name}")

        position_summary = doc.find(".//h2[@style='text-align: center;']")
        position_summary = position_summary.text_content().strip()

        person = context.make("Person")
        person.id = context.make_id(name, role)
        person.add("name", name, lang="tha")
        # apply_translit_full_name(context, person, "tha", name, TRANSLIT_OUTPUT)
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name=role,
            summary=position_summary,
            country="th",
            lang="tha",
            topics=["gov.executive", "gov.national"],
        )
        # apply_translit_full_name(
        #     context, position, "tha", role, TRANSLIT_OUTPUT, POSITION_PROMPT
        # )

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
