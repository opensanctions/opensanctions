from zavod import Context, helpers as h

# from zavod.shed.bods import parse_bods_fh
BASE_URL = "https://rpvs.gov.sk/opendatav2"
ENTITY_DETAILS_ENDPOINT = (
    f"{BASE_URL}/PartneriVerejnehoSektora/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PARTNER_DETAILS_ENDPOINT = f"{BASE_URL}/Partneri/{{id}}?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety"
# TODO: \u0161
BENEFICIAL_OWNERS_ENDPOINT = (
    f"{BASE_URL}/KonecniUzivateliaVyhod/{{id}}?$expand=Partner,PravnaForma,Adresa"
)
PUBLIC_OFFICIALS_ENDPOINT = f"{BASE_URL}/VerejniFunkcionari/{{id}}"

TOTAL_COUNT = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/$count"
FIRST_PAGE = "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skip=0"
# UNSTABLE_LAST_PAGE = (
#     "https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora?$skiptoken=Id-261694"
# )

# TODO do we want to add contry "SK" everywhere?

# def crawl(context: Context) -> None:
#     fn = context.fetch_resource("source.zip", context.data_url)
#     with zipfile.ZipFile(fn, "r") as zf:
#         for name in zf.namelist():
#             if not name.endswith(".json"):
#                 continue
#             with zf.open(name, "r") as fh:
#                 parse_bods_fh(context, fh)

# Exaple URLs
# https://rpvs.gov.sk/opendatav2/PartneriVerejnehoSektora/44?$expand=Partner,PravnaForma,Adresa
# https://rpvs.gov.sk/opendatav2/Partneri/20?$expand=Vymaz,Pokuta,OverenieIdentifikacieKUV,konecniUzivateliaVyhod,verejniFunkcionari,kvalifikovanePodnety
# https://rpvs.gov.sk/opendatav2/KonecniUzivateliaVyhod/45?$expand=Partner,PravnaForma,Adresa


def apply_address(context, entity, address):
    if not address:
        return
    street_name = address.pop("MenoUlice")
    street_number = address.pop("OrientacneCislo", "")
    city = address.pop("Mesto")
    city_code = address.pop("MestoKod", "")
    postal_code = address.pop("Psc")

    address = h.make_address(
        context,
        street=f"{street_name} {street_number}".strip(),
        city=city,
        place=city_code,
        postal_code=postal_code,
        lang="svk",
    )
    h.copy_address(entity, address)


def fetch_related_data(context, related_id, endpoint, headers):
    if not related_id:
        return None
    related_url = endpoint.format(id=related_id)
    response = context.fetch_json(related_url, headers=headers, cache_days=3)
    return response


def emit_relationship(context, entity_data, entity_id, is_pep):
    last_name = entity_data.pop("Priezvisko")
    dob = entity_data.pop("DatumNarodenia")
    ico = entity_data.pop("Ico")
    # One more flag for public officials (used in ownership relationship)
    public_official = entity_data.pop("JeVerejnyCinitel")

    if entity_name := entity_data.pop("ObchodneMeno"):
        schema = "LegalEntity"
        make_id = (ico, entity_name)
        context.log.warning("Legal entity found", entity_data=entity_data)
    elif first_name := entity_data.pop("Meno"):
        schema = "Person"
        make_id = (first_name, last_name, dob)
    else:
        context.log.warn("Unknown schema", entity_data=entity_data)
        return
    # owner_id = owner_data.pop("Id")

    related = context.make(schema)
    related.id = context.make_id(make_id)
    if address := entity_data.pop("Adresa"):
        apply_address(context, related, address)

    if related.schema.name == "LegalEntity":
        related.add("name", entity_name)
        related.add("registrationNumber", ico)
    else:
        h.apply_name(related, first_name=first_name, last_name=last_name)
        h.apply_date(related, "birthDate", dob)
        related.add("title", entity_data.pop("TitulPred"))
        related.add("title", entity_data.pop("TitulZa"))
        if public_official:
            related.add("topics", "role.pep")
            related.add("country", "SK")
        if is_pep:
            # Based on the internal categorization provided by the source
            related.add("topics", "role.pep")
            related.add("country", "SK")
            rel = context.make("UnknownLink")
            rel.id = context.make_id(related.id, "associated with", entity_id)
            rel.add("subject", related.id)
            rel.add("object", entity_id)
            context.emit(rel)
        else:
            own = context.make("Ownership")
            own.id = context.make_id(entity_id, "owned by", related.id)
            own.add("owner", related.id)
            own.add("asset", entity_id)
            context.emit(own)
    context.emit(related)


def process_entry(context, entry, headers):
    entity_id = entry["Id"]
    context.log.info("Fetching entity details", entity_id=entity_id)
    details_url = ENTITY_DETAILS_ENDPOINT.format(id=entity_id)

    entity_data = context.fetch_json(details_url, headers=headers, cache_days=3)
    legal_entity_name = entity_data.get("ObchodneMeno", "")

    entity = context.make("LegalEntity" if legal_entity_name else "Person")
    entity.id = context.make_id(
        entity_data.pop("Id"), entity_data.pop("CisloVlozky", "")
    )
    if entity.schema.name == "Person":
        h.apply_name(
            entity,
            first_name=entity_data.pop("Meno"),
            last_name=entity_data.pop("Priezvisko"),
        )
        h.apply_date(entity, "birthDate", entity_data.pop("DatumNarodenia"))
    else:
        entity.add("name", entity_data.pop("ObchodneMeno"))
        entity.add("registrationNumber", entity_data.pop("Ico"))
        entity.add("legalForm", entity_data.pop("FormaOsoby"))

    if legal_form := entity_data.pop("PravnaForma"):
        entity.add("legalForm", legal_form.pop("Meno"))
        entity.add("classification", legal_form.pop("StatistickyKod"))

    if address := entity_data.pop("Adresa"):
        apply_address(context, entity, address)
    context.emit(entity)
    context.audit_data(
        entity_data, ["@odata.context", "PlatnostOd", "PlatnostDo", "Partner"]
    )

    if partner := entity_data.pop("Partner"):
        partner_id = partner.pop("Id")
        partner_url = PARTNER_DETAILS_ENDPOINT.format(id=partner_id)
        partner_data = context.fetch_json(partner_url, headers=headers)

        # entry_number = partner_data.pop("CisloVlozky")
        for owner in partner_data.pop("KonecniUzivateliaVyhod"):
            owner_data = fetch_related_data(
                context, owner.pop("Id"), BENEFICIAL_OWNERS_ENDPOINT, headers
            )
            emit_relationship(context, owner_data, entity.id, is_pep=False)

        for pep in partner_data.pop("VerejniFunkcionari"):
            pep_data = fetch_related_data(
                context, pep.pop("Id"), PUBLIC_OFFICIALS_ENDPOINT, headers
            )
            context.log.info("Fetched PEP data", pep_data=pep_data)
            emit_relationship(context, pep_data, entity.id, is_pep=True)


def crawl(context: Context):
    headers = {"Accept": "application/json"}
    url = context.data_url
    total_count = context.fetch_json(TOTAL_COUNT)
    if not total_count:
        context.log.warning("Failed to fetch total count")

    processed = 0
    while url and processed < total_count:
        data = context.fetch_json(url, headers=headers, cache_days=3)
        for entry in data.pop("value"):  # Directly iterate over new IDs
            process_entry(context, entry, headers)
            processed += 1
        # It will break when there is no next link
        url = data.pop("@odata.nextLink")
