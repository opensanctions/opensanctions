from zavod import Context
from zavod import helpers as h
from lxml import etree
from collections import defaultdict
import re


def get_xml_link(context: Context) -> str:
    doc = context.fetch_html(context.data_url)
    el = doc.xpath(
        ".//section[@id='documents']//a[contains(@href, 'UK_Sanctions_List.xml')]"
    )
    if len(el) == 0:
        raise ValueError("XML link not found")
    return el[0].get("href")


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


def make_legal_entity(context: Context, designation: etree.Element, entity):
    names = defaultdict(list)
    # Get each name in the designation
    for name_tag in designation.iterfind(".//Names//Name"):
        is_primary = name_tag.find(".//NameType").text.lower() == "primary name"
        # Add the name to the entity
        name = ""
        for name_part in name_tag.iter():
            if name_pat.match(name_part.tag) is not None:
                name += name_part.text
        if is_primary:
            names["primary"].append(name)
        else:
            names["aliases"].append(name)

    unique_id = designation.find("UniqueID")
    if unique_id is None:
        raise Exception("No unique_id found for sanction.")

    if len(names) == 0:
        raise Exception("No names found in designation")

    if len(names["primary"]) > 0:
        entity.id = context.make_slug(names["primary"][0], unique_id.text)
    else:
        entity.id = context.make_slug(names["aliases"][0], unique_id.text)
    for name in names["primary"]:
        h.apply_name(entity, name)
    for name in names["aliases"]:
        h.apply_name(entity, name, alias=True)

    # Add non-latin names to the list
    for name_tag in designation.iterfind(".//NonLatinNames//NameNonLatinScript"):
        h.apply_name(entity, name_tag.text)
    # Add phone numbers
    for phone_number in designation.iterfind(".//PhoneNumbers//PhoneNumber"):
        entity.add("phone", phone_number.text.strip())
    # Add email addresses
    for emails in designation.iterfind(".//EmailAddresses//EmailAddress"):
        for email in re.split(email_split, emails.text):
            entity.add("email", email.strip())
    # Add addresses
    for address in designation.iterfind(".//Addresses//Address"):
        address_text = ""
        for line in address.iter(tag=etree.Element):
            address_text += line.text.strip() + "\n"
        address_country = address.findtext(".//AddressCountry")
        entity.add(
            "address",
            h.make_address(context, address_text.strip(), country=address_country),
        )


def make_person(context: Context, designation: etree.Element, entity):
    individual = designation.find(".//IndividualDetails//Individual")
    if individual is None:
        unique_id = designation.find("UniqueID")
        context.log.info(
            f"No person details found for sanction with ID: {unique_id.text if unique_id is not None else 'Unknown ID'}"
        )
        return
    # Add the date of birth
    for dob in individual.iterfind(".//DOBs//DOB"):
        entity.add("birthDate", h.parse_date(dob.text, formats=["%d/%m/%Y"]))
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


def make_ship(context: Context, designation: etree.Element, entity):
    ship = designation.find(".//ShipDetails//Ship")
    if ship is None:
        unique_id = designation.find("UniqueID")
        context.log.info(
            f"No ship details found for sanction with ID: {unique_id.text if unique_id is not None else 'Unknown ID'}"
        )
        return
    # Add the imonumber
    imo_numbers = []
    for imo in ship.iterfind(".//IMONumbers//IMONumber"):
        imo_numbers.append(imo.text)
    if len(imo_numbers) == 0:
        raise Exception("No IMO number found in ship designation")
    entity.id = imo_numbers[0]
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
        try:
            # Make an entity
            entity = context.make(
                entity_type(designation.find("IndividualEntityShip").text)
            )
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
            for unique_id in designation.iterfind(".//UniqueID"):
                sanction.add("authorityId", unique_id.text)
            # Add the UN reference number
            for reference in designation.iterfind(".//UNReferenceNumber"):
                sanction.add("unscId", reference.text)
            # Add the last updated date
            for date in designation.iterfind(".//LastUpdated"):
                sanction.add(
                    "modifiedAt", h.parse_date(date.text, formats=["%d/%m/%Y"])
                )
            # Add the creation date
            for date in designation.iterfind(".//DateDesignated"):
                sanction.add("startDate", h.parse_date(date.text, formats=["%d/%m/%Y"]))
            # Get the sanctions regime and add it to the entity
            for regime in designation.iterfind(".//RegimeName"):
                sanction.add("program", regime.text)
            # Add the source of the sanction
            for authority in designation.iterfind(".//DesignationSource"):
                sanction.add("authority", authority.text)
            for scope in designation.iterfind(".//SanctionsImposed"):
                sanction.add("provisions", scope.text.split("|"))
            # Add reason as a note
            for info in designation.iterfind(".//OtherInformation"):
                sanction.add("reason", info.text)
            entity.add("topics", "sanction")
            context.emit(entity, target=True)
            context.emit(sanction)
        except Exception as e:
            unique_id = designation.find("UniqueID")
            context.log.error(
                f"Failed to parse designation with id {unique_id.text if unique_id is not None else 'Unknown ID'}: {e}"
            )
            continue
