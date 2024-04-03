import re
import json
from normality import slugify
from typing import Any, Dict, Optional
from pantomime.types import JSON

from zavod import Context, Entity
from zavod import helpers as h

LABELS = {
    "منظمة": "Organization",
    "شخص": "Person",
    "جهة حكومية": "Organization",
    "جهة خاصة": "Company",
}

TYPES = {
    "مرتبطة_بـ": "UnknownLink",  # org->org: Build internal hierarchy (Parent prop)
    "وظيفية": "Employment",  # person->org: has_position - employment
    "إدارية": "Directorship",  # person->org: has_position - directorship
    "عضوية": "Membership",  # person->org: has_position - membership
    "ملكية": "Ownership",  # person->org: has_position - ownership
    "له_نفوذ": "UnknownLink",  # person->org: has influence
    "رابطة_تنظيمية": "Associate",  # person->person: regulatory association
    "رابطة_دم": "Family",  # person->person: blood bond
    "رابطة_مصلحية": "Associate",  # person->person: association of interest
    "رابطة_روحية": "Associate",  # person->person: spiritual bond
    # TODO: command-control relationship
}

IGNORE = ["opensyr-node-1357"]

directorship_titles = [
    "عضو مجلس إدارة",
    "عضو",
    "رئيس",
    "مدير",
    "عضو مجلس الإدارة",
    "رئيس مجلس إدارة",
    "رئيس مجلس الإدارة",
    "مدير مجلس إدارة",
    "رئيس مجلس الإدارة",
]


def parse_date(date: Optional[str]) -> Optional[str]:
    if date is None:
        return None
    date = str(date)
    if date == "10000000":
        return None
    return str(h.parse_formats(date, ["%Y%m%d", "%Y0000", "%Y"]))


def apply(
    entity: Entity,
    props: Dict[str, str],
    prop: str,
    en_field: Optional[str] = None,
    ar_field: Optional[str] = None,
    date: bool = False,
    split: Optional[str] = None,
) -> None:
    for field, lang in ((en_field, "eng"), (ar_field, "ara")):
        if field is None:
            continue
        value: Any = props.pop(field, None)
        if date:
            value = parse_date(value)
        if value is not None and split is not None:
            splitted = re.split("[،;'\\:\"|<,./<>?]", value)
            value = [x.strip() for x in splitted if x]
        entity.add(prop, value, lang=lang, quiet=True)


def extract_node(
    context: Context, node_id: str, label: str, props: Dict[str, str]
) -> None:
    LABELS = {
        "منظمة": "Organization",
        "شخص": "Person",
        "جهة حكومية": "Organization",
        "جهة خاصة": "Company",
    }

    org_type = props.pop("505_نوع_المنظمة", None)
    if org_type == 1:
        label = "جهة حكومية"
    elif org_type == 2:
        label = "جهة خاصة"
    elif org_type is not None:
        label = "منظمة"
    else:
        label = label

    entity = context.make(LABELS[label])
    entity.id = node_id

    # Person
    apply(entity, props, "name", "201_full_name", "101_الاسم_الكامل")
    entity.add("keywords", props.pop("102_وسوم", "").split("|"))  # TODO: map to topics?
    entity.add("keywords", props.pop("202_tags", "").split("|"))  # TODO: map to topics?
    apply(entity, props, "alias", "203_alises", "103_أسماء_بدیلة", split=", ")
    apply(entity, props, "firstName", "204_first_name", "104_الاسم_الأول")
    apply(entity, props, "fatherName", "205_father_name", "105_اسم_الأب")
    apply(entity, props, "middleName", "206_grandparent_name", "106_اسم_الجد")
    apply(entity, props, "lastName", "207_family_name", "107_اسم_العائلة")
    apply(
        entity, props, "birthDate", "208_date_of_birth", "108_تاریخ_المیلاد", date=True
    )
    apply(
        entity, props, "deathDate", "209_date_of_death", "109_تاریخ_الوفاة", date=True
    )
    apply(entity, props, "gender", "210_gender", "110_الجنس")
    apply(entity, props, "nationality", "211_nationality", "111_الجنسیة")
    apply(entity, props, "position", "212_military_rank", "112_الرتبة_العسكرية")
    apply(entity, props, "summary", "214_short_bio", "114_نبذة_مختصرة")
    apply(entity, props, "notes", "215_full_bio", "115_نبذة_مطولة")
    apply(entity, props, "description", "215_full_bio", "115_نبذة_مطولة")
    apply(entity, props, "modifiedAt", "217_last_modified", "117_آخر_تحدیث", date=True)

    # Organization
    apply(entity, props, "name", "601_org_name", "501_اسم_المنظمة")
    entity.add("keywords", props.pop("602_tags", "").split("|"))  # TODO: map to topics?
    entity.add("keywords", props.pop("502_وسوم", "").split("|"))  # TODO: map to topics?
    apply(entity, props, "alias", "603_org_alias", "503_شھرة_المنظمة", split=", ")
    apply(entity, props, "country", "604_country_of_origin", "504_بلد_المنشأ")
    apply(
        entity,
        props,
        "incorporationDate",
        "607_foundation_date",
        "507_تاریخ_التأسيس",
        date=True,
    )
    apply(
        entity,
        props,
        "dissolutionDate",
        "608_dissolution_date",
        "508_تاریخ_الحل",
        date=True,
    )
    apply(entity, props, "summary", "609_short_bio", "509_نبذة_مختصرة")
    apply(entity, props, "notes", "610_full_bio", "510_نبذة_مطولة")
    apply(entity, props, "description", "610_full_bio", "510_نبذة_مطولة")
    apply(entity, props, "modifiedAt", "613_last_modified", "513_آخر_تحدیث", date=True)

    for i in range(11):
        field_name = "link" + str(i)
        apply(entity, props, "sourceUrl", en_field=field_name)

    # Unused labels, but check comments below
    props.pop("213_personality_desc", None)
    props.pop("216_is_publishable", None)
    props.pop("218_img_name", None)  # image file name
    props.pop("219_sector_no", None)
    props.pop(
        "220_ref_links", None
    )  # sources in this format: {source title1~URL address1},{source title2~URL address2}
    props.pop("221_credibility_score", None)
    props.pop("113_وصف_الشخصیة", None)
    props.pop("116_للنشر", None)
    props.pop("118_رابط_الصورة", None)  # image file name
    props.pop("119_رقم_القطاع", None)
    props.pop(
        "120_المصادر", None
    )  # sources in this format: {source title1~URL address1},{source title2~URL address2}
    props.pop("121_الموثوقیة", None)
    props.pop("605_org_type", None)
    props.pop("606_dominance_type", None)
    props.pop("611_sector_no", None)
    props.pop("612_is_publishable", None)
    props.pop("614_img_name", None)  # image file name
    props.pop("615_longitude", None)
    props.pop("616_latitude", None)
    props.pop(
        "617_ref_links", None
    )  # sources in this format: {source title1~URL address1},{source title2~URL address2}
    props.pop("618_credibility_score", None)
    props.pop("505_نوع_المنظمة", None)
    props.pop("506_نوع_السیطرة", None)
    props.pop("511_رقم_القطاع", None)
    props.pop("512_للنشر", None)
    props.pop("514_رابط_الصورة", None)  # image file name
    props.pop("515_خط_الطول", None)
    props.pop("516_دائرة_العرض", None)
    props.pop(
        "517_المصادر", None
    )  # sources in this format: {source title1~URL address1},{source title2~URL address2}
    props.pop("518_الموثوقیة", None)
    props.pop("108_تاریخ_المیلاد_م", None)
    props.pop("008_الجنس", None)
    props.pop("012_النص_التفاعلي", None)
    props.pop("001_الاسم_الكامل", None)
    props.pop("logo_url", None)  # image full URL
    props.pop("009_الجنسية", None)
    props.pop("002_الاسم_الأول", None)
    props.pop("006_تاريخ_الميلاد", None)
    props.pop("109_تاریخ_الوفاة_م", None)
    props.pop("010_الرتبة_العسكرية", None)
    props.pop("007_تاريخ_الوفاة", None)
    props.pop("122_القومية", None)
    props.pop("123_الطائفة", None)
    props.pop("003_اسم_الأب", None)
    props.pop("004_اسم_الجد", None)
    props.pop("002_شهرة_المنظمة", None)
    props.pop("003_بلد_المنشأ", None)
    props.pop("001_اسم_المنظمة", None)
    props.pop("008_النص_التفاعلي", None)
    props.pop("507_تاریخ_التأسيس_م", None)
    props.pop("006_تاريخ_التأسيس", None)
    props.pop("004_نوع_المنظمة", None)
    props.pop("lng", None)  # longitude
    props.pop("lat", None)  # latitude
    props.pop("508_تاریخ_الحل_م", None)

    #     print(entity.properties)

    context.emit(entity)


def extract_relation(
    context: Context,
    rel_id: str,
    source_id: str,
    target_id: str,
    type_: str,
    props: Dict[str, str],
) -> None:

    if type_ in ["له_منصب"]:
        position_type = props.get("041_نوع_المنصب", None)
        position_title = props.get("042_اسم_المنصب", None)
        # position_desc = props.get("043_وصف_المنصب", None)
        if position_type == 1:
            type_ = "وظيفية"
        elif position_type == 2:
            type_ = "وظيفية"
        elif position_type == 3:
            if position_title is not None and any(
                position_title in s for s in directorship_titles
            ):
                type_ = "إدارية"
            else:
                type_ = "ملكية"
        else:
            type_ = "عضوية"

    entity = context.make(TYPES[type_])
    entity.id = rel_id
    if entity.schema.is_a("Family"):
        entity.add("person", source_id)
        entity.add("relative", target_id)
    elif entity.schema.is_a("Associate"):
        entity.add("person", source_id)
        entity.add("associate", target_id)
    elif entity.schema.is_a("UnknownLink"):
        entity.add("subject", source_id)
        entity.add("object", target_id)
    elif entity.schema.is_a("Employment"):
        entity.add("employee", source_id)
        entity.add("employer", target_id)
    elif entity.schema.is_a("Directorship"):
        entity.add("director", source_id)
        entity.add("organization", target_id)
    elif entity.schema.is_a("Ownership"):
        entity.add("owner", source_id)
        entity.add("asset", target_id)
    elif entity.schema.is_a("Membership"):
        entity.add("member", source_id)
        entity.add("organization", target_id)
    else:
        raise ValueError()

    # Family and Associate specific
    apply(entity, props, "relationship", "021_rel_type", "011_نوع_العلاقة")
    apply(entity, props, "relationship", "023_rel_desc", "013_وصف_العلاقة")
    apply(entity, props, "summary", "023_rel_desc", "013_وصف_العلاقة")
    apply(entity, props, "modifiedAt", "026_last_modified", "016_آخر_تحدیث", date=True)

    # Unknown
    apply(entity, props, "summary", "096_influence_desc", "091_وصف_النفوذ")
    apply(entity, props, "summary", "073_rel_desc", "063_وصف_العلاقة")
    apply(entity, props, "role", "071_rel_type", "061_نوع_العلاقة")
    apply(entity, props, "modifiedAt", "078_last_modified", "068_آخر_تحدیث", date=True)
    apply(entity, props, "startDate", "075_start_date", "065_تاریخ_البدایة", date=True)
    apply(entity, props, "endDate", "076_end_date", "066_تاریخ_النھایة", date=True)

    # Employment Directorship Ownership Membership
    apply(entity, props, "startDate", "054_start_date", "044_بدایة_المنصب", date=True)
    apply(entity, props, "endDate", "055_end_date", "045_نھایة_المنصب", date=True)
    apply(entity, props, "modifiedAt", "059_last_modified", "049_آخر_تحدیث", date=True)
    apply(entity, props, "summary", "053_position_desc", "043_وصف_المنصب")
    apply(entity, props, "role", "052_position_title", "042_اسم_المنصب")

    props.pop("025_is_publishable", None)
    props.pop("015_للنشر", None)
    props.pop("022_rel_weight", None)
    props.pop("024_rel_order", None)
    props.pop("014_ترتیب_العلاقة", None)
    props.pop("نوع_العلاقة", None)
    props.pop("012_وزن_العلاقة", None)
    props.pop("وصف_العلاقة", None)
    props.pop("017_المصادر", None)
    props.pop("092_قوة_النفوذ", None)
    props.pop("048_للنشر", None)
    props.pop("097_influence_level", None)
    props.pop("077_is_publishable", None)
    props.pop("072_rel_weight", None)
    props.pop("074_rel_order", None)
    props.pop("066_تاریخ_النھایة_م", None)
    props.pop("067_للنشر", None)
    props.pop("065_تاریخ_البدایة_م", None)
    props.pop("062_وزن_العلاقة", None)
    props.pop("064_ترتیب_العلاقة", None)
    props.pop("069_المصادر", None)
    props.pop("056_is_current_position", None)
    props.pop("058_is_publishable", None)
    props.pop("051_position_type", None)
    props.pop("044_بدایة_المنصب_م", None)
    props.pop("046_المنصب_الحالي", None)
    props.pop("057_rel_order", None)
    props.pop("047_ترتیب_العلاقة", None)
    props.pop("نوع_المنصب", None)
    props.pop("وصف_المنصب", None)
    props.pop("045_نھایة_المنصب_م", None)
    props.pop("050_المصادر", None)

    #     print(entity.properties)

    context.emit(entity)


# # TODO: populate the "LegalEntity:parent" property in Organization, Company, and PublicBody. To achieve this,
# # run the following query which gets the orgA-Affiliated_with->orgB where orgA is the parent of orgB
# # Then retrieve the ID of the two entities (using followthemoney.util.get_entity_id ??) then:
# # orgB.add("parent", orgA_id)
# # I do not know how to do this.. I reviewed the online docs but no luck.


def crawl(context: Context):
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "rb") as fh:
        data = json.load(fh)
        for node in data["nodes"]:
            node_id = f"opensyr-node-{node['id']}"
            if node_id in IGNORE:
                continue
            for label in node["labels"]:
                extract_node(context, node_id, label, node["data"])
        for rel in data["relations"]:
            rel_type = rel["relation_type"]
            rel_source_id = rel["source_id"]
            rel_target_id = rel["target_id"]
            first_id = max(rel_source_id, rel_target_id)
            second_id = min(rel_source_id, rel_target_id)
            rel_id = slugify(f"opensyr-rel-{rel_type}-{first_id}-{second_id}")
            if rel_id is None:
                context.log.warning("Could not generate ID for relation")
                continue
            source_id = f"opensyr-node-{rel['source_id']}"
            target_id = f"opensyr-node-{rel['target_id']}"
            if source_id in IGNORE or target_id in IGNORE:
                continue
            extract_relation(
                context,
                rel_id,
                source_id,
                target_id,
                rel_type,
                rel["relation_data"],
            )
