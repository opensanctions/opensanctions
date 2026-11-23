import json
import re
from rigour.mime.types import JSON, HTML

from zavod import Context
from zavod import helpers as h
from zavod.shed.un_sc import apply_un_name_list
from zavod.extract import zyte_api


TYPES = {"1": "Person", "2": "Organization"}
ALIAS_SPLITS = [";", "original script", "(", ")", "previously listed as"]
# moiListType tells UN-mandated designations apart from Qatar's own ones.
UN_LIST_TYPE = "0"
NATIONAL_LIST_TYPE = "1"
# The source leaves sanctionsDTO.sanctionId blank on part of the records. The
# letters leading the reference number (QDi.001, QLDe.013, ...) name the same
# regime, and agree with sanctionId wherever both are given.
REFERENCE_NUMBER = re.compile(r"^(?P<regime>[A-Z]+)[ie]\.\d+$")
REGIME_BY_REFERENCE_PREFIX = {
    "QD": "1",
    "TA": "2",
    "KP": "3",
    "IR": "4",
    "QLD": "5",
}


def get_regime_key(context: Context, reference_number: str, entity_id: str) -> str:
    """Derive the source's regime code from a reference number.

    Args:
        context: The runner context.
        reference_number: The source's `referenceNumber`, stripped.
        entity_id: The designated entity, for logging.

    Returns:
        The regime code the reference number implies, or an empty string if it
        cannot be derived.
    """
    match = REFERENCE_NUMBER.match(reference_number)
    if match is None:
        context.log.warning(
            "Cannot derive the sanctions regime from the reference number",
            reference_number=reference_number,
            entity_id=entity_id,
        )
        return ""
    prefix = match.group("regime")
    regime = REGIME_BY_REFERENCE_PREFIX.get(prefix)
    if regime is None:
        context.log.warning(
            "Unknown reference number prefix",
            prefix=prefix,
            reference_number=reference_number,
            entity_id=entity_id,
        )
        return ""
    return regime


def crawl(context: Context) -> None:
    # The response says this is HTML even when it really is JSON.
    _, _, _, path = zyte_api.fetch_resource(
        context, "source.json", context.data_url, expected_media_type=HTML
    )
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path) as fh:
        response = fh.read()
        try:
            data = json.loads(response)
        except Exception:
            context.log.error("Failed to parse JSON response", response=response)
            raise

    assert data["totalElements"] > 700, data["totalElements"]

    for item in data["content"]:
        data_id = item.pop("dataId")
        typ = item.pop("typ")
        full_name_en = item.pop("fullNameEn")
        entity = context.make(TYPES[typ])
        entity.id = context.make_slug(data_id, full_name_en)
        assert entity.id is not None, (data_id, full_name_en)

        list_type = item.pop("moiListType")
        reference_number = (item.pop("referenceNumber") or "").strip()
        sanctions_dto = item.pop("sanctionsDTO", {})
        # Blank on the records where the source omits the regime.
        source_program_key = sanctions_dto.pop("sanctionId", "")
        regime_key = source_program_key or get_regime_key(
            context, reference_number, entity.id
        )
        sanction = h.make_sanction(
            context,
            entity,
            source_program_key=source_program_key or None,
            program_key=h.lookup_sanction_program_key(context, regime_key),
        )
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

        h.apply_date(sanction, "listingDate", item.pop("listedOn"))
        if list_type == UN_LIST_TYPE:
            # The reference number of the UN Security Council list this
            # designation is mandated by.
            sanction.add("unscId", reference_number)
        elif list_type == NATIONAL_LIST_TYPE:
            sanction.add("authorityId", reference_number)
        else:
            context.log.warning(
                "Unknown moiListType",
                moi_list_type=list_type,
                reference_number=reference_number,
                entity_id=entity.id,
            )
        sanction.add("program", sanctions_dto.pop("sanctionRegimeEn"), lang="eng")
        sanction.add("program", sanctions_dto.pop("sanctionRegimeAr"), lang="ara")
        context.audit_data(sanctions_dto)

        designation_dto = item.pop("designationDTO", {})
        # The source swaps the language suffixes here: legalBasisAr holds the
        # English text and legalBasisEn the Arabic one.
        sanction.add("reason", designation_dto.pop("legalBasisAr"), lang="eng")
        sanction.add("reason", designation_dto.pop("legalBasisEn"), lang="ara")
        sanction.add("sourceUrl", designation_dto.pop("linkUrl"))
        context.audit_data(
            designation_dto,
            ignore=[
                "desigId",
                "designNumber",
                # Repeats the legal basis, but is blank on most records.
                "linkDecEn",
                "linkDecAr",
                "linkType",
                # The Arabic rendering of linkUrl.
                "linkUrlAr",
            ],
        )
        h.finalize_sanction(context, sanction)

        context.emit(sanction)
        context.emit(entity)
        context.audit_data(
            item,
            ignore=[
                "documentStatus",
                "birthPlaceStatus",
                "birthdateStatus",
                "aliasesStatus",
                # Sometimes left over because we only use the name1,2,3,4 items for Companies
                "fullNameAr",
            ],
        )
