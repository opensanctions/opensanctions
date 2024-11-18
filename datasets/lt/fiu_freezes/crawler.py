from typing import List, Optional
from normality import slugify
from hashlib import sha1

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

POTENTIAL_PAGE = (
    # JT-STR-1373 sanctions list (currently empty)
    "https://fntt.lrv.lt/lt/tarptautines-finansines-sankcijos/JT-STR-1373-sarasas/"
)


# def unblock_validator_1373(el) -> bool:
#     return (
#         "Fizinių ar juridinių asmenų, grupių ir organizacijų įtrauktų"
#         in el.text_content()
#     )


def unblock_validator(el) -> bool:
    return (
        "Fizinio ar juridinio asmens, kurio turtas įšaldytas"
        or "Fizinių ar juridinių asmenų, grupių ir organizacijų įtrauktų"
        in el.text_content()
    )


def crawl_page(context: Context, link: str):
    doc = fetch_html(context, link, unblock_validator, cache_days=3)
    for p in doc.xpath(".//p"):
        p.tail = p.tail + "\n" if p.tail else "\n"
    table = doc.find('.//div[@class="content-block"]//table')
    assert table is not None, "No table found"

    headers: Optional[List[str]] = None
    for row in table.findall(".//tr"):
        cells = [c.text_content() for c in row.findall(".//td")]
        if headers is None:
            headers = [slugify(k, sep="_") for k in cells]
            continue
        data = dict(zip(headers, cells))
        nr = data.pop("nr")
        company_name = (
            data.pop("fizinio_ar_juridinio_asmens_kurio_turtas_isaldytas_pavadinimas")
            .split("\n")[0]
            .strip()
        )
        reg_nr = data.pop("imones_kodas")
        measures = data.pop("isaldyto_turto_rusis").split("\n")
        legal_grounds = data.pop("reglamentas_kurio_pagrindu_taikomas_turto_isaldymas")
        related_entities = data.pop(
            "fizinis_ar_juridinis_asmuo_kuriam_taikomos_tarptautines_sankcijos"
        ).split("\n")
        company = context.make("Company")
        company.id = context.make_slug(nr, company_name)
        company.add("name", company_name)
        company.add("registrationNumber", reg_nr)
        company.add("topics", "sanction")
        context.emit(company, target=True)

        sanction = h.make_sanction(context, company)
        sanction.add("provisions", measures)
        sanction.add("program", legal_grounds)
        context.emit(sanction)

        for related in related_entities:
            if not len(related.strip()):
                continue
            entity = context.make("LegalEntity")
            entity.id = context.make_slug("sanctioned", related)
            entity.add("name", related)
            context.emit(entity)

            rel = context.make("UnknownLink")
            rel.id = context.make_id(company.id, entity.id)
            rel.add("subject", company)
            rel.add("object", entity)
            context.emit(rel)

        context.audit_data(data)


def crawl(context: Context):
    crawl_page(context, context.data_url)

    potential_page = POTENTIAL_PAGE
    h.assert_url_hash(context, potential_page, "")
    # crawl_page(context, potential_page)
