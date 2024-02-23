from xml.etree import ElementTree
from pantomime.types import XML
from zavod import Context
from zavod import helpers as h


def make_person_id(id: str) -> str:
    return f"lv-sanction-person-{id}"


def make_company_id(id: str) -> str:
    return f"lv-sanction-company-{id}"


def crawl_person(context: Context, node: ElementTree):

    entity = context.make("Person")
    entity.id = make_person_id(node.findtext(".//Id"))

    name_node = node.find(".//Name")
    first_name = name_node.findtext(".//FirstName")
    middle_name = name_node.findtext(".//MiddleName")
    last_name = name_node.findtext(".//LastName")
    full_name = name_node.findtext(".//WholeName")

    entity.add("name", full_name)
    entity.add("firstName", first_name)
    entity.add("middleName", middle_name)
    entity.add("lastName", last_name)

    gender = node.findtext(".//Gender")
    if gender.lower() == "v":
        entity.add("gender", "male")
    else:
        context.log.warn(f"Unknown gender - {gender}")

    birth_node = node.find(".//Birth")
    if birth_node is not None:
        birth_date = birth_node.findtext(".//BirthDate")
        birth_country = birth_node.findtext(".//BirthCountry")
        birth_country_code = birth_node.findtext(".//BirthCountryIso2Code")

        entity.add("birthDate", birth_date)
        entity.add("birthPlace", birth_country)
        entity.add("birthCountry", birth_country_code)

    citizenship_node = node.find(".//Citizen")
    if citizenship_node is not None:
        nationality = citizenship_node.findtext(".//CitizenCountry")
        entity.add("nationality", nationality)

    for alias_node in node.findall(".//Alias"):
        alias_full = alias_node.findtext(".//AliasWholeName")
        entity.add("alias", alias_full)

    document_node = node.find(".//Document")
    if document_node is not None:
        document_type = document_node.findtext(".//DocumentType")
        document_number = document_node.findtext(".//DocumentNumber")
        document_country_code = document_node.findtext(".//DocumentCountryIso2Code")

        is_passport = False
        if document_type.lower() == "pase":
            entity.add("passportNumber", document_number)
            is_passport = True
        else:
            context.log.warn(f"Unknown document ID - {document_type}")

        identification_entity = h.make_identification(
            context,
            entity,
            number=document_number,
            country=document_country_code,
            doc_type=document_type,
            passport=is_passport,
        )
        context.emit(identification_entity)

    return entity


def crawl_organization(context: Context, node: ElementTree):
    entity = context.make("Organization")
    entity.id = make_company_id(node.findtext(".//Id"))

    company_name = node.find(".//Name").findtext(".//WholeName")
    entity.add("name", company_name)

    alias_node = node.find(".//Alias")
    if alias_node is not None:
        alias_full = alias_node.findtext(".//AliasWholeName")
        entity.add("alias", alias_full)

    address_nodes = node.findall(".//Address")
    for address_node in address_nodes:
        whole_address = address_node.findtext(".//AddressWhole")
        address_remark = address_node.findtext(".//AddressRemark")
        street = address_node.findtext(".//AddressStreet")
        city = address_node.findtext(".//AddressCity")
        country = address_node.findtext(".//AddressCountry")

        address_entity = h.make_address(
            context,
            full=whole_address,
            remarks=address_remark,
            street=street,
            city=city,
            country=country,
        )
        entity.add("addressEntity", address_entity)
        context.emit(address_entity)

    return entity


def crawl(context: Context):
    path = context.fetch_resource("lv_sanction_list.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)

    for node in doc.findall(".//Entity"):

        entity_type = node.findtext(".//Type")

        if entity_type.lower() == "fp":
            entity = crawl_person(context, node)
        elif entity_type.lower() == "jp":
            entity = crawl_organization(context, node)
        else:
            raise RuntimeError("Cannot not infer entity type")

        entity.add("topics", "sanction")
        sanction = h.make_sanction(context, entity)

        sanction.add("sourceUrl", node.findtext(".//Link"))
        sanction.add("startDate", node.findtext(".//ListedOn"))
        sanction.add("program", node.findtext(".//Program"))
        sanction.add("reason", node.findtext(".//Remark"))

        context.emit(entity, target=True)
