# This is a test to try out Follow The Money with companies house appointments data.

from followthemoney import model
from shared.helpers import company_id, address_id
from shared.helpers import parse_date_appointments, full_address
from shared.readers import read_appointments
from multiprocessing.pool import ThreadPool
import glob

import json

DATA_DIR = "input_files/Prod216_3271"
APPOINTMENT_TYPES = {
    "00": "current-secretary",
    "01": "current-director",
    "04": "current-non-designated-llp-member",
    "05": "current-designated-llp-member",
    "11": "current-judicial-factor",
    "12": "current-receiver-or-manager-appointed-under-the-charities-act",
    "13": "current-manager-appointed-under-the-caice-act",
    "17": "current-se-member-of-administrative-organ",
    "18": "current-se-member-of-supervisory-organ",
    "19": "current-se-member-of-management-organ"
}
COMPANY_STATUS = {
    "C": "converted-or-closed-company",
    "D": "dissolved-company",
    "L": "company-in-liquidation",
    "R": "company-in-receivership",
    "UKN": None
}

WRITE_DIR = "output_files"


def parse_officer(line):
    company_nr = line[0:8]
    comp_id = company_id(company_nr)  # company_id the officer is appointed to

    officer_type = line[24]
    officer_role = APPOINTMENT_TYPES.get(line[10:12].strip(), None)

    if officer_type == "Y":  # Y(es), it's a company!
        officer = model.make_entity("Company")
    else:
        officer = model.make_entity("Person")

    # each officer - be it company or person - has a personal number (PNR).
    # The first 8 should uniquely identify the officer.
    # In ch universe, an officer is identified with a combination of:
    # - PNR + usual residence address (URA) (people officers). Note that by law URA are not publicly available.
    # - PNR + service address (corporate officers).
    # If the URA or the service address change for an appointment then the pnr,
    # or if the officer has multiple appointments, the last 4 digits will be incremented from 0000.

    pnr = line[12:24]

    appointment_start_date = parse_date_appointments(line[32:40].strip(), is_full=True)
    appointment_end_date = parse_date_appointments(line[40:48].strip(), is_full=True)

    partial_dob = parse_date_appointments(line[56:64].strip(), is_full=False)
    full_dob = parse_date_appointments(line[64:72].strip(), is_full=True)

    # variable_data: contains officer’s name, service address, occupation,
    # and nationality, formatted as below:
    # TITLE                      |-> 'title'
    # <FORENAMES                 |-> 'name'
    # <SURNAME                   |-> 'surname'
    # <HONOURS                   |-> 'honours'
    # <CARE OF                   |-> 'service_address_care_of'
    # <PO BOX                    |-> 'service_address_po_box'
    # <ADDRESS LINE 1            |-> 'service_address_line_1'
    # <ADDRESS LINE 2            |-> 'service_address_line_2'
    # <POST TOWN                 |-> 'service_address_post_town'
    # <COUNTY                    |-> 'service_address_county'
    # <COUNTRY                   |-> 'service_address_country'
    # <OCCUPATION                |-> 'occupation'
    # <NATIONALITY               |-> 'nationality'
    # <USUAL RESIDENTIAL COUNTRY |-> 'ura_country'
    # <                          |-> 'filler_b'

    remainder_data = line[76:].rstrip(' \n').split('<')
    remainder_data_nullified = [x.strip() if x.strip() else None for x in remainder_data]
    remainder_fields = [
        'title',
        'name',
        'surname',
        'honours',  # skip for now. Could join with title and add as title.
        'service_address_care_of',
        'service_address_po_box',
        'service_address_line_1',
        'service_address_line_2',
        'service_address_post_town',
        'service_address_county',
        'service_address_country',
        'occupation',
        'nationality',
        'ura_country',
        'filler_b']

    service_address_post_code = line[48:56].strip()
    remainder_dict = dict(zip(remainder_fields, remainder_data_nullified))

    # pnr available for both natural and corporate officers
    officer.add("idNumber", pnr)

    if officer.schema == model.get('Person'):
        name_components = list(filter(None, [remainder_dict.get("name"), remainder_dict.get("surname")]))
        full_name = " ".join(name_components)  # do not pop
        dob = full_dob or partial_dob
        officer.make_id("ch_appointment", pnr, dob, full_name)
        officer.add("birthDate", partial_dob)
        officer.add("birthDate", full_dob)
        officer.add("name", remainder_dict.pop("name"))
        officer.add("title", remainder_dict.pop("title"))
        officer.add("title", remainder_dict.pop("honours"))
        officer.add("lastName", remainder_dict.pop("surname"))
        officer.add("nationality", remainder_dict.pop("nationality"))
        officer.add("country", remainder_dict.pop("ura_country"))
        officer.add("position", remainder_dict.pop("occupation"))

    if officer.schema == model.get('Company'):  # company names are stored as "surname", add both to be safe.
        name_components = list(filter(None, [remainder_dict.get("name"), remainder_dict.get("surname")]))
        full_name = " ".join(name_components)
        officer.make_id("ch_appointment", pnr, full_name)
        officer.add("name", remainder_dict.pop("name"))
        officer.add("name", remainder_dict.pop("surname"))

    # address TODO: should we use zavod make_address() function?
    # https://github.com/opensanctions/zavod/blob/2fca2ad06beb520a4139a51ed7e75cc309316fbc/zavod/parse/addresses.py#L38

    addr = model.make_entity("Address")

    street = remainder_dict.pop("service_address_line_1")
    street2 = remainder_dict.pop("service_address_line_2")
    street3 = remainder_dict.pop("service_address_care_of")
    po_box = remainder_dict.pop("service_address_po_box")
    postal_code = service_address_post_code
    region = remainder_dict.pop("service_address_county")
    city = remainder_dict.pop("service_address_post_town")
    country = remainder_dict.pop("service_address_country")

    addr.add("postOfficeBox", po_box)
    addr.add("street", street)
    addr.add("city", city)
    addr.add("postalCode", postal_code)
    addr.add("region", region)
    addr.add("country", country)

    # use the full address to make the id
    full = full_address(street, street2, street3, po_box, postal_code, region, city, country)
    addr.add("full", full)
    addr.id = address_id(full)

    # add address to officer entity
    officer.add("addressEntity", addr.id)

    link = model.make_entity("Directorship")
    link.make_id("uk-ch-appointment", pnr, company_nr, pnr)

    link.add("director", officer.id)
    link.add("organization", comp_id)
    link.add("role", officer_role)
    link.add("startDate", appointment_start_date)
    link.add("endDate", appointment_end_date)

    # emit jsonl file
    officers_file = "/officers.jsonl"
    address_file = "/addresses_ftm.jsonl"
    appointments_file = "/appointments_ftm.jsonl"

    officer_path = WRITE_DIR + officers_file
    address_path = WRITE_DIR + address_file
    appointments_path = WRITE_DIR + appointments_file

    with open(officer_path, 'a') as f:
        f.write(json.dumps(officer.to_dict()) + "\n")

    with open(address_path, 'a') as f:
        f.write(json.dumps(addr.to_dict()) + "\n")

    with open(appointments_path, 'a') as f:
        f.write(json.dumps(link.to_dict()) + "\n")


def parse_company(line):

    company_nr = line[0:8]
    company_name = line[40:].strip('< \n')
    company_status_code = line[9].replace(" ", "UKN")  # " " means status not known
    company_status = COMPANY_STATUS.get(company_status_code)
    number_of_officers = line[32:36]  # not really needed.

    company = model.make_entity("Company")
    company.id = company_id(company_nr)  # uk-ch company numbers are truly unique. Don't create hash key.
    company.add("status", company_status)
    company.add("name", company_name)

    # emit jsonl file
    companies_file = '/companies_with_appointments_to_them.jsonl'
    path = WRITE_DIR + companies_file

    with open(path, 'a') as f:
        f.write(json.dumps(company.to_dict()) + "\n")


def parse_appointment_line(line):

    # DDDD == first line
    # digit only = last line

    if line.startswith('DDDD') or line.strip().isdigit():
        return

    # if the line is not header or footer, ger record type.
    record_type = line[8]

    if record_type == "1":
        return parse_company(line)
    elif record_type == "2":
        return parse_officer(line)
    else:
        # if we can't identify what the line is, then it's probably broken.

        broken_lines_file = "/broken_lines.jsonl"
        broken_lines_path = WRITE_DIR + broken_lines_file
        with open(broken_lines_path, "a") as fh:
            fh.write(line + "\n")


def process_file(filepath):

    for ix, l in enumerate(read_appointments(filepath)):
        print(f"Appointment line at index {ix}\n")
        parse_appointment_line(l)


def process_directory(dirpath):
    pattern = f'{dirpath}/Prod216*.dat'
    tp = ThreadPool(10)

    # check we have all files we want to process.
    # Then map process_file to pattern.

    for filepath in glob.glob(pattern):
        print(filepath)

    tp.map(process_file, glob.glob(pattern))
    tp.close()
    tp.join()


if __name__ == "__main__":

    process_directory(DATA_DIR)
