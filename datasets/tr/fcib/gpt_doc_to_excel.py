import sys
from csv import DictWriter
from pprint import pprint

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.shed.gpt import run_text_prompt

COLUMNS = [
    "full_name",
    "original_script_name",
    "title",
    "position",
    "date_of_birth_original",
    "date_of_birth_iso",
    "place_of_birth",
    "aliases",
    "nationality",
    "passport_number",
    "national_identity_number",
    "address",
    "additional_information",
    "issues",
]
PROMPT = f"""
Output the following information about this person as a JSON object with the keys
{", ".join(COLUMNS)}. Multiple values should be separated by newlines without numbering
or bullet points. If a field is blank, the value should be null. Passport numbers and
identity numbers should be in the form "number, country, any other information". Each
distinct address should be on a single line. If you think there's a problem with the
data, e.g. split in half or two persons combined, add a short note about that in the
"issues" field, otherwise leave it blank. Keep the original date in date_of_birth_original 
and add a version in ISO-8601 format to date_of_birth_iso. If more than one date is given, 
place one on each line. If you encounter "ValueError: dict contains fields not in fieldnames: 
'101505554, Kuveyt'", add a note in the "issues" field, and leave the field blank. 
"""

infile = sys.argv[1]
persons = open(infile).read().split("Adı Soyadı")

writer = DictWriter(open("outfile.csv", "w"), COLUMNS)
writer.writeheader()

context = Context(Dataset({"name": "fake", "title": "fake"}))
for person in persons:
    try:
        row = run_text_prompt(context, PROMPT, person, cache_days=90)
        writer.writerow(row)
        print("==========")
        pprint(row)
        print("-----")
        print(person)
        print()
    except ValueError as e:
        print(e)
        print(person)
        continue
    except Exception as e:
        print(f"Error processing person: {e}")
        continue
