import csv

from zavod import Context, helpers as h
from zavod.shed.internal_data import fetch_internal_data, list_internal_data


def crawl(context: Context) -> None:
    local_path = context.get_resource_path("companyinfo_v3.csv")
    for blob in list_internal_data("ge_ti_companies/"):
        if blob.endswith(".csv"):
            fetch_internal_data(blob, local_path)
            context.log.info("Parsing: %s" % blob)
            with open(local_path, "r", newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                for index, row in enumerate(reader):
                    if index == 0:
                        print("Header:", row)
                    else:
                        print(row)
                    if index >= 30:
                        break
