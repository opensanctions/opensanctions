# -*- coding: utf-8 -*-
# import re
from pprint import pprint  # noqa

from memorious.helpers import make_id

from opensanctions.models import Entity


# def find_xml_url(context, html_url):
#     html = context.http.get(html_url).html
#     matches = re.findall(r"http://fiu\.gov\.kg/uploads/.*\.xml", html)
#     if len(matches) == 1:
#         return matches[0]


def has_integer_id(row):
    try:
        int(row[0])
        return True
    except Exception:
        return False


def parse_table(table_element):
    rows = table_element.findall('.//row')
    cells = [[cell.text for cell in row.findall('.//cell')] for row in rows]
    return cells


def flatten(list_of_lists):
    return [element for lis in list_of_lists for element in lis]


def extract_data(data):
    data = [parse_table(table) for table in data.findall('.//table')]

    # the first table has 1 column and contains smetadata, todo one of the
    # items could be the date list was released
    assert (len(data[0][0]) == 1)
    data = data[1:]

    # there are two main tables in the document, one with 9 columns containing
    # information about individuals, one with 5 columns containing information
    # about organisations. each of these tables is split up over several pages.
    data = flatten(data)
    individuals = [row for row in data if len(row) == 9]
    organisations = [row for row in data if len(row) == 5]
    assert (2 + len(individuals) + len(organisations) == len(data))

    # only the first row for the two tables should contain a header, data rows
    # contain an integer in the first cell
    assert (not (has_integer_id(individuals[0])))
    individuals = individuals[1:]
    assert (not (has_integer_id(organisations[0])))
    organisations = organisations[1:]

    return individuals, organisations


def handle_individual(context, data):
    header = ["No", "Last Name", "Name", "Middle Name", "Date of birth",
              "Place of birth", "Reason for inclusion",
              "Category of entity", "Date of inclusion"]
    data = {key: value for key, value in zip(header, data)}

    entity_id = make_id(data["Last Name"],
                        data["Middle Name"],
                        data["Name"],
                        data["Reason for inclusion"])
    entity = Entity.create("kg-fiu-national", entity_id)
    entity.type = entity.TYPE_INDIVIDUAL
    entity.last_name = data["Last Name"]
    entity.first_name = data["Name"]
    entity.second_name = data["Middle Name"]
    birth_date = entity.create_birth_date()
    birth_date.date = data["Date of birth"]
    birth_place = entity.create_birth_date()
    birth_place.place = data["Place of birth"]
    entity.program = data["Category of entity"]
    entity.summary = data["Reason for inclusion"]
    entity.listed_at = data["Date of inclusion"]

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def handle_organisation(context, data):
    header = ["No",
              "Name",
              "Reason for inclusion",
              "Category of entity",
              "Date of inclusion"]
    data = {key: value for key, value in zip(header, data)}

    entity_id = make_id(data["Name"], data["Reason for inclusion"])
    entity = Entity.create("kg-fiu-national", entity_id)
    entity.type = entity.TYPE_ENTITY

    if "," in data["Name"]:
        data["Name"] = data["Name"].split(",")
    else:
        data["Name"] = [data["Name"]]
    entity.name = data["Name"][0]
    for alias in data["Name"][1:]:
        entity.create_alias(alias)

    entity.program = data["Category of entity"]
    entity.summary = data["Reason for inclusion"]
    entity.listed_at = data["Date of inclusion"]

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    individuals, organisations = extract_data(res.xml)
    for individual in individuals:
        handle_individual(context, individual)
    for organisation in organisations:
        handle_organisation(context, organisation)
