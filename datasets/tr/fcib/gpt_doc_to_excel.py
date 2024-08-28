import sys
from csv import DictWriter
from pprint import pprint

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.shed.gpt import run_text_prompt
from zavod import helpers as h

COLUMNS = [
    "full_name",
    "original_script_name",
    "known_former_names",
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
    "section",
]


PROMPT = f"""
Output the following information about this organization as a JSON object with the keys
{", ".join(COLUMNS)} which correspond to the field names in Turkish in the text.
The first line is the full name, sometimes with parts of the name
numbered. The other fields have optional field names in the text. Omit name part numbers,
field names, and bullet point symbols like "• Adı :" or
"136-Adı Soyadı". Store the original name into the "full_name" field, the full name in
the original script should be in original_script_name, other names in the aliases field,
and former names in the former_names
field. Original script forms of former names should be treated as distinct values on
their own line, without the "orjinal yazımı" prefix. Multiple values should be separated
by newlines without numbering or bullet points. If a field is blank, the value should be
null. If there appears to be a problem with the data for a record, e.g. split in half or
two persons combined, add a short note about that in the "issues" field, otherwise leave
the issues field blank. Only return text that occurred in the provided text. Do not add
additional information.
"""

infile = sys.argv[1]
full_text = open(infile).read()
full_text = full_text.replace("\u2028", "\n")  # Line separator
full_text = full_text.replace("\u2029", "\n")  # Paragraph separator
records = h.multi_split(
    full_text,
    [
        "\n\nAdı Soyadı",
        "\n\nAdı",
        "\n \nAdı Soyadı",
    ],
)

writer = DictWriter(open("output.csv", "w"), COLUMNS)
writer.writeheader()

context = Context(Dataset({"name": "fake", "title": "fake"}))
for idx, record in enumerate(records):
    try:
        row = run_text_prompt(context, PROMPT, record)
        writer.writerow(row)
        context.cache.flush()
        print(f"===== {idx} =====")
        pprint(row)
        print("-----")
        print(record)
        print()
    except Exception as e:
        print(f"Error processing record {idx}\n{record}: {e}")
        # Leave a blank to manually enter if needed.
        writer.writerow({k: None for k in COLUMNS})
        continue
