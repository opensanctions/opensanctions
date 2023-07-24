import csv
from normality import slugify
from pantomime.types import CSV

from zavod import Context
from opensanctions import helpers as h


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            data = {slugify(k, sep="_"): v for k, v in row.items()}
            nr = data.pop("nr")
            company_name = data.pop(
                "fizinio_ar_juridinio_asmens_kurio_turtas_isaldytas_pavadinimas"
            )
            reg_nr = data.pop("imones_kodas")
            measures = data.pop("isaldyto_turto_rusis").split("\n")
            legal_grounds = data.pop(
                "reglamentas_kurio_pagrindu_taikomas_turto_isaldymas"
            )
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
