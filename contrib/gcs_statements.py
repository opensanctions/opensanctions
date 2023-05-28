from google.cloud.storage import Bucket, Client, Blob
from nomenklatura.statement.serialize import write_json_statement

from opensanctions.core import Dataset
from opensanctions.core.collection import Collection
from opensanctions.core.db import engine_read
from opensanctions.core.statements import all_statements

client = Client()
bucket = client.get_bucket("data.opensanctions.org")

# print(bucket)

# blob = bucket.get_blob("datasets/latest/us_ofac_sdn/entities.ftm.json")
# # blob.download_to_filename("xx.json")

for dataset in Dataset.all():
    if dataset.TYPE == Collection.TYPE:
        continue
    print(dataset)
    blob_name = f"datasets/latest/{dataset.name}/statements.json"
    blob = bucket.get_blob(blob_name)
    if blob is not None:
        print("Exists:", blob_name)
        continue

    tmp_path = "data/tmp.json"
    with open("data/tmp.json", "wb") as fh:
        with engine_read() as conn:
            stmts = all_statements(conn, dataset=dataset, external=True)
            for idx, stmt in enumerate(stmts):
                if idx > 0 and idx % 50000 == 0:
                    print("Writing", dataset, idx)
                write_json_statement(fh, stmt)

    blob = bucket.blob(blob_name)
    blob.upload_from_filename(tmp_path)
    print("Uploaded:", blob_name)
