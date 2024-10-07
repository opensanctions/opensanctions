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
    for field in doc.findall(".//div[@class='col-lg-2 col-md-2 col-sm-6 col-xs-12']"):
        # Extract the text from the first <h4> (name)
        name_field = field.find(".//h4[1]")
        name = name_field.text_content().strip() if name_field is not None else None
        if not name:
            context.log.warning(f"Name missing for {field}")

        # Extract the text from the second <h4> (position)
        role_field = field.find(".//h4[2]")
        role = role_field.text_content().strip() if role_field is not None else None
        if not role:
            context.log.warning(f"Position missing for {name}")

        position_summary = doc.find(".//h2[@style = 'text-align: center;']")
        position_summary = position_summary.text_content().strip()

        if not name or not role:
            continue

        person = context.make("Person")
        person.id = context.make_id(name, role)
        person.add("name", name, lang="tha")
        apply_translit_full_name(context, person, "kat", name, TRANSLIT_OUTPUT)
        person.add("position", role)
        person.add("topics", "role.pep")

        position = h.make_position(
            context,
            name="Member of the Cabinet",
            summary=position_summary,
            country="th",
            lang="tha",
            topics=["gov.executive", "gov.national"],
        )
        apply_translit_full_name(
            context, position, "kat", role, TRANSLIT_OUTPUT, POSITION_PROMPT
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
