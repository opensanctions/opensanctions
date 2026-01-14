import re
import csv
from typing import Optional, List

from rigour.mime.types import CSV
from zavod.helpers.xml import ElementOrTree

from zavod import Context, Entity
from zavod import helpers as h


EMAIL_SPLIT = re.compile(r"[; ]")
PATTERNS = [
    (r"^INN:?\s*(\d{10}|\d{12})\s*$", "innCode"),
    (r"^OGRN:?\s*(\d{13}|\d{15})\s*$", "ogrnCode"),
    (r"^KPP:?\s*(\d{9})\s*$", "kppCode"),
    (r"^(?:BIC|BIK):?\s*(\d{9})\s*$", "bikCode"),
    (
        r"^(?:SWIFT(?:/BIC)?|BIC):?\s*([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\s*$",
        "swiftBic",
    ),
    (r"^TIN:?\s*(\d{10})\s*$", "taxNumber"),
]


def get_xml_link(context: Context) -> str:
    doc = context.fetch_html(context.data_url)
    for el in h.xpath_elements(
        doc, ".//section[@id='documents']//a[contains(@href, 'UK-Sanctions-List.xml')]"
    ):
        return h.xpath_string(el, "./@href")
    raise ValueError("XML link not found")


def get_csv_link(context: Context) -> str:
    doc = context.fetch_html(context.data_url)
    xq = ".//section[@id='documents']//a[contains(@href, 'UK-Sanctions-List.csv')]"
    for el in h.xpath_elements(doc, xq):
        href = el.get("href")
        if href is None:
            continue
        return href
    raise ValueError("CSV link not found")


def add_reg_property(entity: Entity, prop: str, value: str, original: str):
    """Helper to add property with Company cast for specific codes."""
    if prop in ("kppCode", "bikCode"):
        entity.add_cast("Company", prop, value, original_value=original)
    else:
        entity.add(prop, value, original_value=original)


def apply_reg_number(context: Context, entity: Entity, reg_number: str):
    for pattern, prop in PATTERNS:
        if match := re.match(pattern, reg_number, re.IGNORECASE):
            value = match.group(1)
            add_reg_property(entity, prop, value, reg_number)
            return
    # If no pattern matched, try to look it up
    result = context.lookup("reg_number", reg_number, warn_unmatched=True)
    if result is None:
        return
    if result.props:
        for prop, value in result.props.items():
            add_reg_property(entity, prop, value, reg_number)
    elif result.value == "SAME":
        entity.add("registrationNumber", reg_number)
    else:
        context.log.warn(
            "Ambiguous registration number lookup result",
            reg_number=reg_number,
            result=result,
        )


def parse_companies(context: Context, value: Optional[str]) -> List[str]:
    if not value:
        return []
    result = context.lookup("companies", value, warn_unmatched=True)
    if result is None:
        return []
    if result.value == "SAME":
        return [value]
    return result.values


def entity_type(type: str) -> str:
    match type.lower():
        case "individual":
            return "Person"
        case "entity":
            return "Organization"
        case "ship":
            return "Vessel"
        case _:
            raise ValueError("Unknown entity type")


def xml_make_legal_entity(context: Context, designation: ElementOrTree, entity: Entity):
    # Add phone numbers
    for phone_number in designation.iterfind(".//PhoneNumbers//PhoneNumber"):
        if phone_number.text is not None:
            entity.add("phone", phone_number.text.strip())
    # Add email addresses
    for emails in designation.iterfind(".//EmailAddresses//EmailAddress"):
        if emails.text is None:
            continue
        for email in re.split(EMAIL_SPLIT, emails.text):
            entity.add("email", email.strip())
    # Add addresses
    for address in designation.iterfind(".//Addresses//Address"):
        postcode, pobox = h.postcode_pobox(address.findtext("./AddressPostalCode"))
        addr = h.make_address(
            context,
            street=address.findtext("./AddressLine1"),
            street2=address.findtext("./AddressLine2"),
            street3=address.findtext("./AddressLine3"),
            place=address.findtext("./AddressLine4"),
            region=address.findtext("./AddressLine5"),
            city=address.findtext("./AddressLine6"),
            postal_code=postcode,
            po_box=pobox,
            country=address.findtext("./AddressCountry"),
        )
        h.copy_address(entity, addr)


def xml_make_person(context: Context, designation: ElementOrTree, entity: Entity):
    for individual in designation.findall(".//IndividualDetails//Individual"):
        # Add the date of birth
        for dob in individual.iterfind(".//DOBs//DOB"):
            h.apply_date(entity, "birthDate", dob.text)
        # Add titles
        entity.add("title", individual.findtext(".//Title"))
        # Add birthplace
        for details in individual.iterfind(".//BirthDetails//Location"):
            location = ""
            if town := details.findtext(".//TownOfBirth"):
                location += town + "\n"
                entity.add("birthPlace", town)
            if country := details.findtext(".//CountryOfBirth"):
                location += country
            entity.add("birthPlace", location)
        # Add nationalities
        for nationality in individual.iterfind(".//Nationalities//Nationality"):
            entity.add("nationality", nationality.text)
        # Add positions
        for position in individual.iterfind(".//Positions//Position"):
            entity.add("position", position.text)


def xml_make_ship(context: Context, designation: ElementOrTree, entity: Entity):
    for ship in designation.findall(".//ShipDetails//Ship"):
        # Add the imonumber
        for imo in ship.iterfind(".//IMONumbers//IMONumber"):
            entity.add("imoNumber", imo.text)

        # Add the owners
        for owner in ship.iterfind(".//CurrentOwnerOperators//CurrentOwnerOperator"):
            owner_entity = context.make("Organization")
            owner_entity.id = context.make_slug("named", owner.text)
            owner_entity.add("name", owner.text)
            context.emit(owner_entity)

            own = context.make("Ownership")
            own.id = context.make_id("ownership", owner_entity.id, entity.id)
            own.add("owner", owner_entity.id)
            own.add("asset", entity.id)
            context.emit(own)

        # Add the registration number
        # Add the type
        for ship_type in ship.iterfind(".//TypeOfShipDetails//TypeOfShip"):
            entity.add("type", ship_type.text)
        # Add the flag
        for flag in ship.iterfind(
            ".//CurrentBelievedFlagOfShips//CurrentBelievedFlagOfShip"
        ):
            entity.add("flag", flag.text)


def crawl_xml(context: Context):
    # Get the XML file
    url = get_xml_link(context)
    xml = context.fetch_resource("source.xml", url)
    et = context.parse_resource_xml(xml)
    # Get each designation
    for designation in et.iter("Designation"):
        ent_type = designation.find("IndividualEntityShip")
        if ent_type is None or ent_type.text is None:
            context.log.error("No entity type found for designation", doc=designation)
            continue
        # Make an entity
        entity = context.make(entity_type(ent_type.text))
        unique_id = designation.findtext("UniqueID")
        if unique_id is None:
            context.log.error("No unique_id found for sanction", doc=designation)
            continue
        entity.id = context.make_slug(unique_id)

        # Get each name in the designation
        for name_tag in designation.iterfind(".//Names//Name"):
            name_type = name_tag.findtext(".//NameType")
            name_res = context.lookup("name_type", name_type, warn_unmatched=True)
            if name_res is None:
                continue

            # name1 is always a given name
            # name6 is always a family name
            # name2-name5 are sometimes given names, sometimes patro-/matronymic names
            # We play it safe here and put into more specific properties only what we're sure of
            name1 = name_tag.findtext("./Name1")
            entity.add("firstName", name1, quiet=True, lang="eng")
            name6 = name_tag.findtext("./Name6")
            entity.add("lastName", name6, quiet=True, lang="eng")

            full_name = h.make_name(
                full=name_tag.findtext("./Name"),
                name1=name1,
                name2=name_tag.findtext("./Name2"),
                name3=name_tag.findtext("./Name3"),
                name4=name_tag.findtext("./Name4"),
                name5=name_tag.findtext("./Name5"),
                tail_name=name6,
            )
            entity.add(name_res.prop, full_name, lang="eng")

        if not entity.has("name"):
            context.log.info("No names found for entity", id=entity.id)

        # Add non-latin names to the list
        for name_tag in designation.iterfind(".//NonLatinNames"):
            # context.inspect(name_tag)
            lang = name_tag.findtext(".//NonLatinScriptLanguage")
            lang_code = context.lookup_value("languages", lang)
            if lang is not None and lang_code is None:
                context.log.warn(
                    "Unknown language, please add to languages lookup.", language=lang
                )
                continue
            h.apply_name(
                entity,
                name_tag.findtext("./NameNonLatinScript"),
                lang=lang_code,
                alias=True,
            )

        try:
            if entity.schema.label in ["Person", "Organization"]:
                xml_make_legal_entity(context, designation, entity)
            # If it is an individual
            elif entity.schema.label == "Person":
                xml_make_person(context, designation, entity)
            # If it is a ship
            elif entity.schema.label == "Vessel":
                xml_make_ship(context, designation, entity)
            else:
                context.log.warn(
                    "Unknown entity type",
                    id=entity.id,
                    type=entity.schema.label,
                    doc=designation,
                )
                continue

            # Extract the sanctions regime
            regime_name = [
                regime.text.strip()
                for regime in designation.iterfind(".//RegimeName")
                if regime.text
            ]
            assert len(regime_name) == 1, regime_name
            # Make a sanctions entity
            sanction = h.make_sanction(
                context,
                entity,
                program_name=regime_name[0],
                source_program_key=regime_name[0],
                program_key=h.lookup_sanction_program_key(context, regime_name[0]),
            )
            # Add the unique ID
            sanction.add("authorityId", unique_id)
            # Add the UN reference number
            for reference in designation.iterfind(".//UNReferenceNumber"):
                sanction.add("unscId", reference.text)
            # Add the last updated date
            for date in designation.iterfind(".//LastUpdated"):
                h.apply_date(sanction, "modifiedAt", date.text)
            # Add the creation date
            for date in designation.iterfind(".//DateDesignated"):
                h.apply_date(sanction, "startDate", date.text)
            # Add the source of the sanction
            for authority in designation.iterfind(".//DesignationSource"):
                sanction.add("authority", authority.text)
            for scope in designation.iterfind(".//SanctionsImposed"):
                if scope.text is not None:
                    sanction.add("provisions", scope.text.split("|"))
            # Add reason as a note
            for info in designation.iterfind(".//OtherInformation"):
                entity.add("notes", info.text)
            for info in designation.iterfind(".//UKStatementofReasons"):
                sanction.add("reason", info.text)
            entity.add("topics", "sanction")
            context.emit(entity)
            context.emit(sanction)
        except ValueError as e:
            context.log.error(f"Failed to parse designation with id {unique_id}: {e}")


def csv_make_legal_entity(context: Context, row: dict, entity: Entity):
    entity.add("phone", row.pop("Phone number"))
    entity.add("email", h.multi_split(row.pop("Email address"), [", ", "; "]))
    entity.add("website", row.pop("Website"))
    # Mix of legal forms and sectors
    entity.add("summary", row.pop("Type of entity"))

    reg_number = row.pop("Business registration number (s)")
    if reg_number:
        apply_reg_number(context, entity, reg_number)

    postcode, pobox = h.postcode_pobox(row.pop("Address Postal Code"))
    addr = h.make_address(
        context,
        street=row.pop("Address Line 1"),
        street2=row.pop("Address Line 2"),
        street3=row.pop("Address Line 3"),
        place=row.pop("Address Line 4"),
        region=row.pop("Address Line 5"),
        city=row.pop("Address Line 6"),
        postal_code=postcode,
        po_box=pobox,
        country=row.pop("Address Country"),
    )
    h.copy_address(entity, addr)

    parent_names = row.pop("Parent company")
    for name in parse_companies(context, parent_names):
        parent = context.make("Organization")
        parent.id = context.make_slug("named", name)
        parent.add("name", name)
        context.emit(parent)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(parent.id, "owns", entity.id)
        ownership.add("owner", parent)
        ownership.add("asset", entity)
        context.emit(ownership)

    for name in parse_companies(context, row.pop("Subsidiaries")):
        subsidiary = context.make("Company")
        subsidiary.id = context.make_slug("named", name)
        subsidiary.add("name", name)
        context.emit(subsidiary)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(entity.id, "owns", subsidiary.id)
        ownership.add("owner", entity)
        ownership.add("asset", subsidiary)
        context.emit(ownership)


def csv_make_person(context: Context, row: dict, entity: Entity):
    csv_make_legal_entity(context, row, entity)
    h.apply_date(entity, "birthDate", row.pop("D.O.B"))
    entity.add("title", row.pop("Title"))
    entity.add("gender", row.pop("Gender"))
    entity.add("birthPlace", row.pop("Town of birth"))
    entity.add("birthPlace", row.pop("Country of birth"))
    entity.add("idNumber", row.pop("National Identifier number"))
    entity.add("nationality", row.pop("Nationality(/ies)"))
    entity.add("position", row.pop("Position"))

    passport_no = row.pop("Passport number")
    passport = context.make("Passport")
    passport.id = context.make_id(passport_no, entity.id)
    passport.add("number", passport_no)
    passport.add("summary", row.pop("Passport additional information"))
    passport.add("holder", entity.id)
    context.emit(passport)


def csv_make_ship(context: Context, row: dict, entity: Entity):
    entity.add("type", row.pop("Type of ship"))
    entity.add("flag", row.pop("Current believed flag of ship"))
    entity.add("pastFlags", row.pop("Previous flags"))
    entity.add("tonnage", row.pop("Tonnage of ship"))
    entity.add("buildDate", row.pop("Year Built"))
    entity.add("registrationNumber", row.pop("Hull identification number (HIN)"))
    entity.add("imoNumber", row.pop("IMO number"))
    owner = row.pop("Current owner/operator (s)")
    for owner in parse_companies(context, owner.strip()):
        owner_entity = context.make("Organization")
        owner_entity.id = context.make_slug("named", owner)
        owner_entity.add("name", owner)
        context.emit(owner_entity)

        own = context.make("Ownership")
        own.id = context.make_id("ownership", owner_entity.id, entity.id)
        own.add("owner", owner_entity.id)
        own.add("asset", entity.id)
        context.emit(own)

    previous_owner = row.pop("Previous owner/operator (s)")
    for previous_owner in parse_companies(context, previous_owner.strip()):
        previous_owner_entity = context.make("Organization")
        previous_owner_entity.id = context.make_slug("named", previous_owner)
        previous_owner_entity.add("name", previous_owner)
        context.emit(previous_owner_entity)

        own = context.make("UnknownLink")
        own.id = context.make_id("ownership", previous_owner_entity.id, entity.id)
        own.add("subject", previous_owner_entity.id)
        own.add("object", entity.id)
        context.emit(own)


def get_name_prop(
    context: Context,
    entity: Entity,
    name: Optional[str],
    name_type: str,
    alias_strength: str,
) -> Optional[str]:
    if name is None:
        return None

    name_res = context.lookup("name_type", name_type, warn_unmatched=True)
    if name_res is None:
        return None
    name_prop = name_res.prop
    if not name_res.is_alias:
        return name_prop

    if alias_strength != "":
        if context.lookup_value("is_weak_alias", alias_strength, warn_unmatched=True):
            return "weakAlias"
        else:
            return name_prop

    # If alias_strength is blank, consider overriding name_prop from name_type based on heuristics

    if entity.schema.is_a("Person") and " " not in name:
        return "weakAlias"
    if (
        entity.schema.is_a("Organization")
        and " " not in name
        and len(name) < 7
        and name.isupper()
    ):
        return "abbreviation"
    if entity.schema.is_a("Organization") and " " not in name and len(name) < 6:
        return "weakAlias"

    return name_prop


def crawl_csv(context: Context):
    csv_url = get_csv_link(context)
    path = context.fetch_resource("source.csv", csv_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r", encoding="utf-8") as fh:
        # Skip the first metadata row
        next(fh)
        for row in csv.DictReader(fh):
            entity = context.make(entity_type(row.pop("Designation Type")))
            unique_id = row.pop("Unique ID")
            entity.id = context.make_slug(unique_id)

            # name1 is always a given name
            # name6 is always a family name or org name
            # name2-name5 are sometimes given names, sometimes patro-/matronymic names
            # We play it safe here and put into more specific properties only what we're sure of
            given_name = row.pop("Name 1")
            last_name = row.pop("Name 6")
            full_name = h.make_name(
                name1=given_name,
                name2=row.pop("Name 2"),
                name3=row.pop("Name 3"),
                name4=row.pop("Name 4"),
                name5=row.pop("Name 5"),
                tail_name=last_name,
            )

            name_type = row.pop("Name type")
            alias_strength = row.pop("Alias strength")
            name_prop = get_name_prop(
                context, entity, full_name, name_type, alias_strength
            )
            if name_prop is None:
                continue

            entity.add(name_prop, full_name, lang="eng", original_value=full_name)
            entity.add("firstName", given_name, quiet=True, lang="eng")
            entity.add("lastName", last_name, quiet=True, lang="eng")

            # Add non-latin name
            non_latin_name = row.pop("Name non-latin script")
            non_latin_lang = row.pop("Non-latin script language")
            if non_latin_name:
                lang_code = context.lookup_value(
                    "languages", non_latin_lang, warn_unmatched=True
                )
                h.apply_name(entity, non_latin_name, lang=lang_code, alias=True)

            if entity.schema.label == "Organization":
                csv_make_legal_entity(context, row, entity)
            elif entity.schema.label == "Person":
                csv_make_person(context, row, entity)
            elif entity.schema.label == "Vessel":
                csv_make_ship(context, row, entity)
            else:
                context.log.warn(
                    "Unknown entity type",
                    id=entity.id,
                    type=entity.schema.label,
                    row=row,
                )
                continue

            regime_name = row.pop("Regime Name")

            sanction = h.make_sanction(
                context,
                entity,
                program_name=regime_name,
                source_program_key=regime_name,
                program_key=h.lookup_sanction_program_key(context, regime_name),
            )

            sanction.add("authorityId", unique_id)
            sanction.add("authorityId", row.pop("OFSI Group ID"))
            sanction.add("unscId", row.pop("UN Reference Number"))
            sanction.set("authority", row.pop("Designation source"))
            sanction.add("reason", row.pop("UK Statement of Reasons"))
            h.apply_date(sanction, "modifiedAt", row.pop("Last Updated"))
            h.apply_date(sanction, "startDate", row.pop("Date Designated"))

            entity.add("notes", row.pop("Other Information"))
            entity.add("topics", "sanction")

            sanctions_imposed = row.pop("Sanctions Imposed")
            if sanctions_imposed:
                sanction.add("provisions", sanctions_imposed.split("|"))

            context.emit(entity)
            context.emit(sanction)

            context.audit_data(
                row,
                [
                    "National Identifier additional information",
                    "Non-latin script type",
                    "Length of ship",
                ],
            )


def crawl(context: Context):
    crawl_xml(context)
    crawl_csv(context)
