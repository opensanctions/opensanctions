from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html
from zavod.util import Element


def crawl_page(
    context: Context,
    link: str,
    unblock_validator: str,
    program_key: str,
    required: bool = True,
) -> Element:
    doc = fetch_html(context, link, unblock_validator, cache_days=3)
    # Inject newlines after each <p> so multi-value cells (one value per
    # paragraph) can be split on "\n" below.
    for p in h.xpath_elements(doc, ".//p"):
        p.tail = p.tail + "\n" if p.tail else "\n"
    tables = h.xpath_elements(doc, './/div[@class="content-block"]//table')
    if len(tables) == 0:
        if required:
            raise ValueError(f"No table found in {link}")
        else:
            return doc

    assert len(tables) == 1, f"Expected exactly one table in {link}"
    # Headers are in <td>, not <th>. squash=False preserves the \n separators
    # injected above for splitting multi-value cells.
    for row in h.parse_html_table(tables[0], header_tag="td"):
        data = {k: h.element_text(v, squash=False) for k, v in row.items()}
        nr = data.pop("nr")
        company_name_raw = (
            data.pop("fizinio_ar_juridinio_asmens_kurio_turtas_isaldytas_pavadinimas")
            .split("\n")[0]
            .strip()
        )
        company_name = company_name_raw
        cleaning_props = {}
        if "(" in company_name:
            res = context.lookup("company_name", company_name)
            if res is None:
                context.log.warn("Company name might need cleaning", name=company_name)
            else:
                company_name = res.name
                cleaning_props = res.props
        reg_nr = data.pop("imones_kodas")
        measures = data.pop("isaldyto_turto_rusis").split("\n")
        legal_grounds = data.pop("reglamentas_kurio_pagrindu_taikomas_turto_isaldymas")
        related_entities = data.pop(
            "fizinis_ar_juridinis_asmuo_kuriam_taikomos_tarptautines_sankcijos"
        ).split("\n")
        company = context.make("Company")
        company.id = context.make_slug(nr, company_name_raw)
        company.add("name", company_name, original_value=company_name_raw)
        company.add("registrationNumber", reg_nr)
        company.add("topics", "sanction")
        for prop, value in cleaning_props.items():
            company.add(prop, value)
        context.emit(company)

        sanction = h.make_sanction(context, company, program_key=program_key)
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


def crawl(context: Context) -> None:
    assert context.dataset.model.url
    # Detect if new sanctions programs are added
    index_doc = fetch_html(
        context,
        context.dataset.model.url,
        ".//*[contains(text(), 'Tarptautinės finansinės sankcijos. Įgyvendinimas')]",
        cache_days=1,
    )
    index_main = h.xpath_element(index_doc, ".//main")
    h.assert_dom_hash(index_main, "73a409804f5bc0d84cc145859b010bf6a223199e")
    # LIST OF LEGAL ENTITIES OR OTHER ORGANIZATIONS WITHOUT LEGAL PERSONAL STATUS THAT ARE OWNED
    # OR CONTROLLED BY A SANCTIONED ENTITY
    crawl_page(
        context,
        "https://fntt.lrv.lt/lt/tarptautines-finansines-sankcijos/sankcionuotu-asmenu-sarasas/",
        ".//*[contains(text(), 'Fizinio ar juridinio asmens, kurio turtas įšaldytas')]",
        program_key="LT-SL",
    )
    # List of natural or legal persons, groups and entities included in the list of natural or legal persons,
    # groups and entities associated with terrorist acts, whose funds and other financial assets must be frozen,
    # established on the basis of UNSCR 1373 (2001) (as amended)
    unsc_1373_doc = crawl_page(
        context,
        "https://fntt.lrv.lt/lt/tarptautines-finansines-sankcijos/JT-STR-1373-sarasas/",
        ".//*[contains(text(), 'JT ST rezoliucijoje 1373 (2001)')]",
        program_key="LT-UNSCR1373",
        required=False,
    )
    unsc_1373_main = h.xpath_element(unsc_1373_doc, ".//main")
    h.assert_dom_hash(unsc_1373_main, "cfcb88db7b9d67150bbc114423f64a467a32fbc3")
