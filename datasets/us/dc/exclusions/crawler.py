from zavod import Context, helpers as h


def log_messy_names(context, name_value):
    if any(term in name_value for term in ("aka", "a/k/a")):
        context.log.warning("Name needs to be cleaned up:", name=name_value)


def lookup_position(context, principal):
    position = None
    if "President" in principal:
        result = context.lookup("director", principal)
        if result and result.names:
            principal = result.names[0].get("principal")
            position = result.names[0].get("position")
        if not position:
            context.log.warning(
                "Could not find position for director", director=principal
            )
    return principal, position


def emit_directorship(context, entity_id, principal, position):
    director = context.make("Person")
    director.id = context.make_id(principal)
    director.add("name", principal)
    log_messy_names(context, director.get("name")[0])
    director.add("position", position)
    context.emit(director)

    dir = context.make("Directorship")
    dir.id = context.make_id(director.id, "director of", entity_id)
    dir.add("organization", entity_id)
    dir.add("director", director.id)
    context.emit(dir)


def crawl_row(context, row):
    schema = "Person" if row.get("name_of_individual") else "Company"
    name_raw = row.pop("name_of_individual", None) or row.pop("name_of_company", None)
    assert name_raw is not None, "Entity must have a name"
    end_date = row.pop("expiration_date", row.pop("termination_date", None))
    assert end_date is not None, "Missing expiration or termination date for sanction"
    address = row.pop("principal_address")

    entity = context.make(schema)
    entity.id = context.make_id(name_raw, address)
    entity.add("name", name_raw)
    if entity.schema.is_a("Person"):
        log_messy_names(context, entity.get("name")[0])
    else:
        principal = row.pop("principals", None)
        if principal:
            principal, position = lookup_position(context, principal)
            emit_directorship(context, entity.id, principal, position)

    entity.add("country", "us")
    entity.add("address", address)

    sanction = h.make_sanction(context, entity)
    sanction.set("authority", row.pop("agency_instituting_the_action", None))
    sanction.add("reason", row.pop("reason_for_the_action", None))
    h.apply_date(sanction, "startDate", row.pop("action_date"))
    h.apply_date(sanction, "endDate", end_date)
    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    tables = doc.xpath(".//table")
    # We expect 2 current and 2 historical tables
    assert len(tables) == 4, "Expected 4 tables, found %d" % len(tables)

    for table in tables:
        for row in h.parse_html_table(table, header_tag="td"):
            str_row = h.cells_to_str(row)
            crawl_row(context, str_row)
