from typing import Any, Dict

from zavod import Context, helpers as h

RESTRICTION_NOTE = (
    "{organization} is restricted from procurement from {subject} "
    "from {start_date} {until_clause} due to ownership or management role "
    "of a public official in {organization} or their family member. "
    "This is a preventative restriction by the Komisija za preprečevanje korupcije "
    "and implies no wrongdoing."
)


def crawl_entity(context: Context, record: Dict[str, Any]):
    subject_name = record.pop("ps_naziv")
    registration_number = record.pop("ps_maticna")
    country = record.pop("drzava")
    start_date = record.pop("datum_od")
    end_date = record.pop("datum_do")
    organization_name = record.pop("organ")
    organization_number = record.pop("sifra_pu")

    if not subject_name and not registration_number:
        context.log.info(
            "Subject name and registration number not found", record=record
        )
        return

    legal_entity = context.make("LegalEntity")
    legal_entity.id = context.make_id(registration_number or subject_name)
    legal_entity.add("name", subject_name)
    legal_entity.add("registrationNumber", registration_number)
    legal_entity.add("taxNumber", record.pop("ps_davcna"))
    legal_entity.add("topics", "debarment")
    legal_entity.add("country", country)

    address = h.make_address(
        context,
        street=record.pop("ps_naslov"),
        place=record.pop("ps_posta"),
        country=country,
    )
    h.copy_address(legal_entity, address)

    if registration_number and organization_number:
        legal_entity.add(
            "sourceUrl",
            f"https://registri.kpk-rs.si/registri/omejitve_poslovanja/seznam/#3={organization_number}&8={registration_number}",
        )

    legal_entity.add(
        "program",
        RESTRICTION_NOTE.format(
            organization=organization_name,
            subject=subject_name,
            start_date=start_date,
            until_clause=f"until {end_date}" if end_date else "until further notice",
        ),
    )

    sanction = h.make_sanction(context, legal_entity)
    h.apply_date(sanction, "startDate", start_date)
    h.apply_date(sanction, "endDate", end_date)

    context.emit(legal_entity)
    context.emit(sanction)

    context.audit_data(
        record, ignore=["tip_omejitve", "posta", "naslov", "maticna", "davcna"]
    )


def crawl(context: Context):
    response = context.fetch_json(context.data_url, cache_days=1)
    for record in response:
        crawl_entity(context, record)
