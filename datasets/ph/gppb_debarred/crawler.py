from typing import Any
from enum import Enum

from zavod import Context
from zavod import helpers as h
from rigour.text.stopwords import is_nullword


class DebarmentStatus(Enum):
    BLACKLISTED_ENTITIES = "temporary debarment"
    PERMANENT_BLACKLISTED_ENTITIES = "permanent debarment"


IGNORE = [
    "category",
    "blacklisting_order_no",
    "date_blacklisting",
    "approved_budget_cost",
    "stage_violation",
    "project_location",
    "bo_signatory",
    "procuring_entity",
    "additional_saction",
    "managing_officer",
    "saction_imposed",
    "temp_start_date",
    "temp_end_date",
    "is_removed_by_system",
    "is_temp_removed_by_system",
    "created_date",
]


def crawl_record(
    context: Context, record: dict[str, Any], status: DebarmentStatus
) -> None:
    name = record.pop("blacklisted_entity")
    assert name is not None, "Record without blacklisted_entity"

    reg_no = record.pop("philgeps_reg_no")
    address = record.pop("address")
    project = record.pop("project")
    offenses = record.pop("offenses")

    entity = context.make("Organization")
    entity.id = context.make_id(name, address, reg_no)
    if reg_no is not None and not is_nullword(reg_no):
        entity.add("registrationNumber", reg_no)

    entity.add("name", name)
    entity.add("address", address)
    entity.add("country", "ph")
    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("status", status.value)
    h.apply_date(sanction, "startDate", record.pop("start_date"))
    h.apply_date(sanction, "modifiedAt", record.pop("updated_date"))

    end_date = record.pop("end_date")
    if status != DebarmentStatus.PERMANENT_BLACKLISTED_ENTITIES:
        h.apply_date(sanction, "endDate", end_date)

    for offense in offenses.values():
        sanction.add("reason", offense)
    if project is None:
        context.log.warn("Record without project name", name=name)
    # contract the entity got blacklisted over:
    sanction.add("reason", project)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(record, ignore=IGNORE)


def crawl(context: Context) -> None:
    for category in DebarmentStatus:
        context.log.info(f"Crawling category {category.name}")
        data = context.fetch_json(
            context.data_url + f"?category={category.name}", cache_days=1
        )
        assert isinstance(data, list), f"Expected list, got {type(data)}"
        for record in data:
            crawl_record(context, record, category)
