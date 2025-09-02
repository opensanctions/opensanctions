import json
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.shed.un_sc import apply_un_name_list

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
        entity.add("alias", h.multi_split(item.pop("aliases", ""), ALIAS_SPLITS))
        entity.add("topics", "sanction")

        names_en = [
            name
            for name in [
                item.pop("firstNameEN"),
                item.pop("secondNameEN"),
                item.pop("thirdNameEN"),
                item.pop("fourthNameEN"),
            ]
            if name is not None and name != ""
        ]
        names_ar = [
            name
            for name in [
                item.pop("firstNameAR"),
                item.pop("secondNameAR"),
                item.pop("thirdNameAR"),
                item.pop("fourthNameAR"),
            ]
            if name is not None and name != ""
        ]

        dob_format = item.pop("dobFormat")
        if entity.schema.is_a("Person"):
            entity.add("passportNumber", item.pop("passportNo"))
            entity.add("idNumber", item.pop("qid"))
            entity.add("nationality", item.pop("nationality"))
            h.apply_date(entity, "birthDate", dob_format)

            # Because this is mostly a copy of the UN list, names follow the same semantics
            apply_un_name_list(context, entity, names_en, lang="eng")
            apply_un_name_list(context, entity, names_ar, lang="ara")

            # For Person, fullNameAR/EN are concatenations of the names, but it doesn't hurt to add them twice
            # because they get deduped at the statement level.
            entity.add("name", full_name_en, lang="eng")
            entity.add("name", item.pop("fullNameAr"), lang="ara")
        elif entity.schema.is_a("Organization"):
            # For Organization, fullNameAR/EN are concatenations of the names, but that's dumb for organizations
            # because we end up with names like
            # "Al-Khalidi Exchange Al-Khalidi Jewelry Company  Al-Khalidi Money Transfer Office "
            # so we just don't add them.
            entity.add("name", names_ar, lang="ara")
            entity.add("name", names_en, lang="eng")

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
