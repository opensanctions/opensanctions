import json
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h

TYPES = {"1": "Person", "2": "Organization"}
ALIAS_SPLITS = [";", "original script", "(", ")", "previously listed as"]


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)

    assert data["totalElements"] > 700
    for item in data["content"]:
        data_id = item.pop("dataId")
        typ = item.pop("typ")
        full_name_en = item.pop("fullNameEn")
        entity = context.make(TYPES[typ])
        entity.id = context.make_slug(data_id, full_name_en)
        sanction = h.make_sanction(context, entity)
        entity.add("name", full_name_en, lang="eng")
        entity.add("name", item.pop("fullNameAr"), lang="ara")
        entity.add("alias", h.multi_split(item.pop("aliases", ""), ALIAS_SPLITS))
        entity.add("topics", "sanction")

        first_name_ar = item.pop("firstNameAR")
        first_name_en = item.pop("firstNameEN")
        second_name_ar = item.pop("secondNameAR")
        second_name_en = item.pop("secondNameEN")
        third_name_ar = item.pop("thirdNameAR")
        third_name_en = item.pop("thirdNameEN")
        fourth_name_ar = item.pop("fourthNameAR")
        fourth_name_en = item.pop("fourthNameEN")
        dob_format = item.pop("dobFormat")
        if entity.schema.is_a("Person"):
            entity.add("passportNumber", item.pop("passportNo"))
            entity.add("idNumber", item.pop("qid"))
            entity.add("nationality", item.pop("nationality"))
            h.apply_date(entity, "birthDate", dob_format)
            entity.add("firstName", first_name_ar, lang="ara")
            entity.add("firstName", first_name_en, lang="eng")
            entity.add("secondName", second_name_ar, lang="ara")
            entity.add("secondName", second_name_en, lang="eng")
            entity.add("middleName", third_name_ar, lang="ara")
            entity.add("middleName", third_name_en, lang="eng")
            entity.add("middleName", fourth_name_ar, lang="ara")
            entity.add("middleName", fourth_name_en, lang="eng")
        elif entity.schema.is_a("Organization"):
            entity.add("alias", first_name_ar, lang="ara")
            entity.add("alias", first_name_en, lang="eng")
            entity.add("alias", second_name_ar, lang="ara")
            entity.add("alias", second_name_en, lang="eng")
            entity.add("registrationNumber", item.pop("passportNo"))
            entity.add("registrationNumber", item.pop("qid"))
            entity.add("jurisdiction", item.pop("nationality"))
        # print(entity.to_dict())

        sanction.add("listingDate", item.pop("listedOn"))
        sanction.add("unscId", item.pop("referenceNumber"))
        sanctions_dto = item.pop("sanctionsDTO", {})
        sanction.add("authorityId", sanctions_dto.pop("sanctionId", None), lang="eng")
        sanction.add("program", sanctions_dto.pop("sanctionRegimeEn"), lang="eng")
        sanction.add("program", sanctions_dto.pop("sanctionRegimeAr"), lang="ara")
        context.audit_data(sanctions_dto)

        item.pop("designationDTO", {})
        # h.audit_data(designation_dto)

        context.emit(entity)
        context.audit_data(
            item,
            ignore=[
                "documentStatus",
                "birthPlaceStatus",
                "birthdateStatus",
                "aliasesStatus",
                "moiListType",
            ],
        )
