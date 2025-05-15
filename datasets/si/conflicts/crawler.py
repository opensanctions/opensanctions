from typing import Any, Dict

from zavod import Context, helpers as h

# let's not make Sanction entities for these - this dataset should be handled
# a bit carefully because it's preventative, not punitive. While we use Sanction
# entities to describe other debarments, we know some users sometimes over-react
# when they see them.

IGNORE = [
    "restriction_type",
    "org_place",
    "org_address",
    "org_reg_number",
    "org_tax_number",
    "entity_place",
    "entity_address",
]
RESTRICTION_NOTE = (
    "{organization} is restricted from procurement from {subject} "
    "from {start_date} {until_clause} due to ownership or management role "
    "of a public official in {organization} or their family member. "
    "This is a preventative restriction by the Komisija za preprečevanje korupcije "
    "and implies no wrongdoing."
)


def rename_record(context, entry):
    result = {}
    for old_key, value in entry.items():
        new_key = context.lookup_value("columns", old_key)
        if new_key is None:
            context.log.warning("Unknown column title", column=old_key)
            new_key = old_key
        result[new_key] = value
    return result


def crawl_entity(context: Context, record: Dict[str, Any]):
    subject_name = record.pop("entity_name")
    registration_number = record.pop("entity_reg_number")
    country = record.pop("country")
    start_date = record.pop("start_date")
    end_date = record.pop("end_date")
    org_name = record.pop("org_name")
    org_internal_id = record.pop("org_internal_id")

    # There are some records with no subject name or registration number
    if not subject_name and not registration_number:
        return

    legal_entity = context.make("LegalEntity")
    legal_entity.id = context.make_id(registration_number or subject_name)
    legal_entity.add("name", subject_name)
    legal_entity.add("registrationNumber", registration_number)
    legal_entity.add("taxNumber", record.pop("entity_tax_number"))
    legal_entity.add("topics", "debarment")
    legal_entity.add("country", country)
    legal_entity.add(
        "program",
        RESTRICTION_NOTE.format(
            organization=org_name,
            subject=subject_name,
            start_date=start_date,
            until_clause=f"until {end_date}" if end_date else "until further notice",
        ),
    )
    if registration_number and org_internal_id:
        legal_entity.add(
            "sourceUrl",
            f"https://registri.kpk-rs.si/registri/omejitve_poslovanja/seznam/#3={org_internal_id}&8={registration_number}",
        )
    context.emit(legal_entity)

    context.audit_data(record, IGNORE)


def crawl(context: Context):
    response = context.fetch_json(context.data_url, cache_days=1)
    for record in response:
        renamed_record = rename_record(context, record)
        crawl_entity(context, renamed_record)
