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
        ".//div[@style='min-height: 480; max-height: 600; display: flex; align-items: center; flex-direction: column; justify-content: center; width: 100%;']"  # to be updated
    )
    persons = main_container.findall(
        ".//div[@style='min-height: 360; max-height: 500;']"
    )
    persons.append(prime_minister)

    for person in persons:
        # Find all h4, h5, h6 tags in a single call
        h_tags = [".//h4", ".//h5", ".//h6"]
        for h_tag in h_tags:
            name_fields = person.findall(h_tag)
            if name_fields:
                for field in name_fields:
                    text_content = field.text_content().strip()
                    print(f"Field: {text_content}")

                collected_text = " ".join(
                    [
                        field.text_content().strip()
                        for field in name_fields
                        if field is not None
                    ]
                )
                # print(f"Collected text: {collected_text}")

        for text in h.multi_split(
            collected_text, ["นายกรัฐมนตรี", "รองนายกรัฐมนตรี", " รัฐมนตรีว่าการ"]
        ):  # prime minister, deputy prime minister, minister
            name = text.strip()[0]
            role = text.strip()[1]

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
