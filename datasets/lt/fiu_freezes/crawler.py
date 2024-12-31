from typing import List, Optional
from normality import slugify

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_page(
    context: Context,
    link: str,
    unblock_validator: str,
    required: bool = True,
):
    doc = fetch_html(context, link, unblock_validator, cache_days=3)
    for p in doc.xpath(".//p"):
        p.tail = p.tail + "\n" if p.tail else "\n"
    table = doc.find('.//div[@class="content-block"]//table')
    if table is None:
        if required:
            raise ValueError(f"No table found in {link}")
        else:
            return doc

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
    return doc


def crawl(context: Context):
    # Detect if new sanctions programs are added
    index_doc = fetch_html(
        context,
        context.dataset.url,
        ".//*[contains(text(), 'Tarptautinės finansinės sankcijos. Įgyvendinimas')]",
        cache_days=1,
    )
    index_main = index_doc.xpath(".//main")
    assert len(index_main) == 1, len(index_main)
    h.assert_dom_hash(index_main[0], "dcdfba83ce9c8cab447a6eb9fa0ee91b4d2f4df5")

    crawl_page(
        context,
        "https://fntt.lrv.lt/lt/tarptautines-finansines-sankcijos/sankcionuotu-asmenu-sarasas/",
        ".//*[contains(text(), 'Fizinio ar juridinio asmens, kurio turtas įšaldytas')]",
    )
    unsc_1373_doc = crawl_page(
        context,
        "https://fntt.lrv.lt/lt/tarptautines-finansines-sankcijos/JT-STR-1373-sarasas/",
        ".//*[contains(text(), 'JT ST rezoliucijoje 1373 (2001)')]",
        required=False,
    )
    unsc_1373_main = unsc_1373_doc.xpath(".//main")
    assert len(unsc_1373_main) == 1, len(unsc_1373_main)
    h.assert_dom_hash(unsc_1373_main[0], "b4c16e6f4ad7609e736b6971952bf2e79b7ec188")
