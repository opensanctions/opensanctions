from typing import Any

from zavod import Context
from zavod import helpers as h


SANCTION_LABELS: dict[str, str] = {
    "ONE_YEAR_BLACKLISTING": "One-year blacklisting",
    "TWO_YEAR_BLACKLISTING": "Two-year blacklisting",
}


def format_offenses(offenses: dict[str, str] | None) -> str | None:
    """Convert the offenses dict into a human-readable reason string."""
    if not offenses:
        return None
    parts = [f"[{code}] {desc}" for code, desc in offenses.items()]
    return "; ".join(parts)


def crawl_record(context: Context, record: dict[str, Any]) -> None:
    name = record.pop("blacklisted_entity")
    assert name is not None, "Record without blacklisted_entity"

    reg_no = record.pop("philgeps_reg_no")
    address = record.pop("address")
    managing_officer = record.pop("managing_officer")
    procuring_entity = record.pop("procuring_entity")
    project = record.pop("project")
    offenses = record.pop("offenses")
    sanction_type = record.pop("saction_imposed")
    start_date = record.pop("start_date")
    end_date = record.pop("end_date")

    # Consumed but not used
    record.pop("category", None)
    record.pop("blacklisting_order_no", None)
    record.pop("date_blacklisting", None)
    record.pop("approved_budget_cost", None)
    record.pop("stage_violation", None)
    record.pop("project_location", None)
    record.pop("additional_saction", None)
    record.pop("bo_signatory", None)
    record.pop("temp_start_date", None)
    record.pop("temp_end_date", None)
    record.pop("is_removed_by_system", None)
    record.pop("is_temp_removed_by_system", None)
    record.pop("updated_date", None)
    record.pop("created_date", None)

    entity = context.make("Organization")
    if reg_no is None or reg_no == "N/A":
        entity.id = context.make_id(name, address)
    else:
        entity.id = context.make_slug(reg_no)
    entity.add("name", name)
    entity.add("address", address)
    if reg_no is not None and reg_no != "N/A":
        entity.add("registrationNumber", reg_no)
    entity.add("country", "ph")
    entity.add("topics", "debarment")

    assert sanction_type in SANCTION_LABELS, f"Unknown sanction: {sanction_type!r}"
    program_name = SANCTION_LABELS[sanction_type]
    reason = format_offenses(offenses)

    sanction = h.make_sanction(
        context,
        entity,
        key=start_date,
        program_name=program_name,
        source_program_key=sanction_type,
        program_key=h.lookup_sanction_program_key(context, sanction_type),
        start_date=start_date,
        end_date=end_date,
    )
    sanction.add("reason", reason)
    sanction.add("provisions", sanction_type)
    if procuring_entity is not None:
        sanction.add("summary", f"Blacklisted by {procuring_entity}")

    if project is not None:
        sanction.add("description", f"Project: {project}")

    context.emit(entity)
    context.emit(sanction)

    if managing_officer is not None:
        officer = context.make("Person")
        officer.id = context.make_id(managing_officer, reg_no)
        officer.add("name", managing_officer)
        officer.add("country", "ph")
        context.emit(officer)

        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, "officer", officer.id)
        link.add("subject", entity)
        link.add("object", officer)
        link.add("role", "Managing Officer")
        context.emit(link)

    context.audit_data(record)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=1)
    assert isinstance(data, list), f"Expected list, got {type(data)}"
    for record in data:
        crawl_record(context, record)
