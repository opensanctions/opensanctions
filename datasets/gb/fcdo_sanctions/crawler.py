import re

from zavod import Context, Entity
from zavod import helpers as h
from zavod.helpers.xml import ElementOrTree


def get_xml_link(context: Context) -> str:
    doc = context.fetch_html(context.data_url)
    xq = ".//section[@id='documents']//a[contains(@href, 'UK_Sanctions_List.xml')]"
    for el in doc.xpath(xq):
        return el.get("href")
    raise ValueError("XML link not found")


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


name_pat = re.compile(r"Name(\d+)")
email_split = re.compile(r"[; ]")


def make_legal_entity(context: Context, designation: ElementOrTree, entity: Entity):
    # Add phone numbers
    for phone_number in designation.iterfind(".//PhoneNumbers//PhoneNumber"):
        if phone_number.text is not None:
            entity.add("phone", phone_number.text.strip())
    # Add email addresses
    for emails in designation.iterfind(".//EmailAddresses//EmailAddress"):
        if emails.text is None:
            continue
        for email in re.split(email_split, emails.text):
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


def make_person(context: Context, designation: ElementOrTree, entity: Entity):
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


def make_ship(context: Context, designation: ElementOrTree, entity: Entity):
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
            entity.add("owner", owner_entity)

        # Add the registration number
        # Add the type
        for ship_type in ship.iterfind(".//TypeOfShipDetails//TypeOfShip"):
            entity.add("type", ship_type.text)
        # Add the flag
        for flag in ship.iterfind(
            ".//CurrentBelievedFlagOfShips//CurrentBelievedFlagOfShip"
        ):
            entity.add("flag", flag.text)


def crawl(context: Context):
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
            name_prop = context.lookup_value("name_type", name_type)
            if name_prop is None:
                context.log.warn("Unknown name type", name_type=name_type)
                continue

            # context.inspect(name_tag)
            h.apply_name(
                entity,
                full=name_tag.findtext("./Name"),
                name1=name_tag.findtext("./Name1"),
                name2=name_tag.findtext("./Name2"),
                name3=name_tag.findtext("./Name3"),
                name4=name_tag.findtext("./Name4"),
                name5=name_tag.findtext("./Name5"),
                last_name=name_tag.findtext("./Name6"),
                lang="eng",
                name_prop=name_prop,
                quiet=True,
            )

        if not entity.has("name"):
            context.log.info("No names found for entity", id=entity.id)

        # Add non-latin names to the list
        for name_tag in designation.iterfind(".//NonLatinNames"):
            # context.inspect(name_tag)
            lang = name_tag.findtext(".//NonLatinScriptLanguage")
            lang_code = context.lookup_value("languages", lang)
            if lang is not None and lang_code is None:
                context.log.warn("Unknown language", language=lang)
                continue
            h.apply_name(
                entity, name_tag.findtext("./NameNonLatinScript"), lang=lang_code
            )

        try:
            if entity.schema.label in ["Person", "Organization"]:
                make_legal_entity(context, designation, entity)
            # If it is an individual
            if entity.schema.label == "Person":
                make_person(context, designation, entity)
            # If it is a ship
            if entity.schema.label == "Vessel":
                make_ship(context, designation, entity)
            # Make a sanctions entity
            sanction = h.make_sanction(context, entity)
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
            # Get the sanctions regime and add it to the entity
            for regime in designation.iterfind(".//RegimeName"):
                sanction.add("program", regime.text)
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
