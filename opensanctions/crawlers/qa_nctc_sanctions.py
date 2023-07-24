import json
from typing import List
from pantomime.types import JSON
from prefixdate import parse_formats

from zavod import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split

TYPES = {"1": "Person", "2": "Organization"}
FORMATS = ["%Y-%m-%d", "%Y-%m-%d-%H", "%d/%m/%Y", "X_%Y_X_X", "%d-%m-%Y", "%Y"]


def parse_dates(text: str) -> List[str]:
    if not len(text):
        return []
    dates = set()
    type_, text = text.split("_", 1)
    if text == "X_X_X_X":
        return []
    for part in multi_split(text, [":", ";", "؛", " to "]):
        part = part.replace("___", " ")
        part = part.replace("_X_X_X", " ")
        part = part.strip()
        if part == "00":
            continue
        parsed = parse_formats(part, FORMATS)
        if parsed.text is not None:
            dates.add(parsed.text)
        else:
            dates.update(h.extract_years(part))
    # if not len(dates):
    #     print(text)
    # return [text]
    return dates


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)

    assert data["totalElements"] > 800
    for item in data["content"]:
        data_id = item.pop("dataId")
        typ = item.pop("typ")
        full_name_en = item.pop("fullNameEn")
        entity = context.make(TYPES[typ])
        entity.id = context.make_slug(data_id, full_name_en)
        sanction = h.make_sanction(context, entity)
        entity.add("name", full_name_en, lang="eng")
        entity.add("name", item.pop("fullNameAr"), lang="ara")
        entity.add("alias", item.pop("aliases", None))
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
        dobs = parse_dates(dob_format)
        if entity.schema.is_a("Person"):
            entity.add("passportNumber", item.pop("passportNo"))
            entity.add("idNumber", item.pop("qid"))
            entity.add("nationality", item.pop("nationality"))
            entity.add("birthDate", dobs, original_value=dob_format)
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

        designation_dto = item.pop("designationDTO", {})
        # h.audit_data(designation_dto)

        context.emit(entity, target=True)
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
