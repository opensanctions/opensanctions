from collections import defaultdict
from datetime import date, datetime
import os
from pathlib import Path
import tempfile
from typing import Any, Generator, List, Iterable, Iterator, Dict
from zipfile import ZipFile
from dataclasses import dataclass

import numpy as np
import pandas
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from google.cloud.storage import Client  # type: ignore

from egrul_xml import parse_xml
from schema import company_record_schema

LOCAL_BUCKET_CACHE_DIR = Path(
    os.environ.get("LOCAL_BUCKET_CACHE_DIR", tempfile.gettempdir())
)

SOURCE_DATA_BUCKET_NAME = "egrul.opensanctions.org"

# Format versions differ slightly but not enough to affect our parsing.
# 406/407 overlap heavily; we switch to 407 at 2025-01-01.
# 407/408 overlap Feb 10 – Mar 7 2026; we switch to 408 at 2026-03-01.
SOURCE_DATA_PREFIX_406 = "egrul/EGRUL_406/"
SOURCE_DATA_PREFIX_407 = "egrul/EGRUL_407/"
SOURCE_DATA_PREFIX_408 = "egrul/EGRUL_408/"


@dataclass
class BlobURL:
    """A wrapper around blob URLs that can be pickled for Spark workers."""

    url: str

    def _split_url(self) -> tuple[str, str]:
        """Split the URL into bucket name and blob name."""
        # URL format: gs://bucket-name/path/to/blob
        assert self.url.startswith("gs://")
        split = self.url[5:].split("/", 1)
        assert len(split) == 2, f"Invalid Blob URL: {self.url}"
        return split[0], split[1]

    @property
    def bucket_name(self) -> str:
        """Extract the bucket name from the URL."""
        return self._split_url()[0]

    @property
    def name(self) -> str:
        """Extract the blob name from the URL."""
        return self._split_url()[1]

    def __str__(self) -> str:
        return self.url


def get_local_archive_path(blob_url: BlobURL) -> Path:
    return LOCAL_BUCKET_CACHE_DIR / str(blob_url.name)


def get_archive_date_from_blob_url(blob_url: BlobURL) -> date:
    """Gets an archive date from the blob URL."""
    # blob_url.name format: "egrul/EGRUL_406/01.01.2022_FULL/EGRUL_FULL_2022-01-01_214.zip"
    path_parts = blob_url.name.split("/")
    if len(path_parts) < 2:
        raise ValueError(f"Invalid blob name format: {blob_url.name}")

    dirname = path_parts[-2]  # Get the directory name before the zip file
    # 01-01 has a _FULL suffix
    dirname = dirname.rstrip("_FULL")
    return datetime.strptime(dirname, "%d.%m.%Y").date()


def aggregate_archives_by_date(
    archive_blobs: Iterable[BlobURL],
) -> Dict[date, List[BlobURL]]:
    archives_by_date = defaultdict(list)
    for archive_blob in archive_blobs:
        archive_date = get_archive_date_from_blob_url(archive_blob)
        archives_by_date[archive_date].append(archive_blob)
    return archives_by_date


def list_archives(bucket_name: str, prefix: str) -> List[BlobURL]:
    """List all archive blobs from Google Cloud Storage and convert to BlobURL objects."""
    client = Client()
    bucket = client.get_bucket(bucket_name)

    return [
        BlobURL(f"gs://{bucket_name}/{blob.name}")
        for blob in bucket.list_blobs(prefix=prefix)
        if blob.name.endswith(".zip")
    ]


def list_archives_by_date(
    start_date: date = date(2022, 1, 1),
) -> List[tuple[date, List[BlobURL]]]:
    """List all source archives across format versions, grouped and sorted by date.

    Each format version covers a date range (versions overlap; we pick a cutover):
      - EGRUL_406: before 2025-01-01
      - EGRUL_407: 2025-01-01 to 2026-03-01
      - EGRUL_408: 2026-03-01 onward
    """
    archives_406 = [
        a
        for a in list_archives(SOURCE_DATA_BUCKET_NAME, SOURCE_DATA_PREFIX_406)
        if get_archive_date_from_blob_url(a) < date(2025, 1, 1)
    ]
    archives_407 = [
        a
        for a in list_archives(SOURCE_DATA_BUCKET_NAME, SOURCE_DATA_PREFIX_407)
        if date(2025, 1, 1) <= get_archive_date_from_blob_url(a) < date(2026, 3, 1)
    ]
    archives_408 = [
        a
        for a in list_archives(SOURCE_DATA_BUCKET_NAME, SOURCE_DATA_PREFIX_408)
        if get_archive_date_from_blob_url(a) >= date(2026, 3, 1)
    ]
    archives = archives_406 + archives_407 + archives_408

    return [
        (d, a)
        for d, a in sorted(aggregate_archives_by_date(archives).items())
        if d >= start_date
    ]


def crawl_archive(blob_url: BlobURL) -> Generator[dict[str, Any], None, None]:
    # Lazy import to avoid circular import with generate.py
    from generate import get_context

    data_date = get_archive_date_from_blob_url(blob_url)
    context = get_context(data_date)

    local_archive_path = get_local_archive_path(blob_url)
    # TODO: Since we cache persistently locally (for running on Leon's machine),
    # maybe a checksum comparison with the remote blob would be a good idea.
    if not os.path.exists(local_archive_path):
        context.log.info("Downloading archive: %s" % blob_url)
        client = Client()
        bucket = client.get_bucket(blob_url.bucket_name)
        blob = bucket.blob(blob_url.name)
        # mkdir -p the directory for the archive
        local_archive_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(local_archive_path)

    context.log.info(
        "Opening local archive: %s (cache of %s)" % (local_archive_path, blob_url)
    )

    try:
        with ZipFile(local_archive_path, "r") as zip:
            for name in zip.namelist():
                if not name.lower().endswith(".xml"):
                    continue
                with zip.open(name, "r") as fh:
                    for e in parse_xml(context, fh):
                        yield e
    finally:
        # Don't clean up the temporary file, for now this is being run on Leon's machine and it's
        # okay to just have them cached.
        # os.unlink(local_archive_path)
        pass


def merge_duplicate_company_records(df: DataFrame) -> DataFrame:
    """Deduplicate companies that have the same ID in the given DataFrame.

    Note: this is NOT the historical merging across dumps (that happens a layer above, in
    update_companies_from_new_companies). Here we just clean up an artifact within a single
    dump where the same company ID can appear multiple times — these appear to be dissolved
    and reopened companies, some bureaucratic artifact.
    """

    dupe_ids = df.groupBy("id").count().where(col("count") > 1).drop("count")
    dupes = df.join(dupe_ids, on="id", how="inner")
    non_dupes = df.join(dupe_ids, on="id", how="left_anti")

    def _merge_companies(pdf: pandas.DataFrame) -> pandas.DataFrame:
        # TODO(Leon Handreke): Implement a more sophisticated merge strategy. For now, all of them
        # seem to be dissolved and reopened companies, some bureaucratic artifact.
        # Magic from
        # https://stackoverflow.com/questions/45469417/sort-by-column-sub-values-in-pandas
        newest = pdf.iloc[
            np.argsort([x.get("incorporation_date") for x in pdf.legal_entity])
        ]
        return newest[:1]

    deduped = dupes.groupBy("id").applyInPandas(_merge_companies, company_record_schema)
    # We do this fancy union stuff to avoid running expensive python on the non-dupes
    return non_dupes.union(deduped)


def _parse_archives_udf(
    batches: Iterable[pandas.DataFrame],
) -> Iterator[pandas.DataFrame]:
    """mapInPandas worker: take batches of [url] rows, yield batches of parsed company records.

    Flushes every N rows to bound per-worker memory, since our records contain
    nested arrays (ownerships/directorships/etc.) and can be large.
    """
    FLUSH_EVERY = 10_000

    rows: list[dict[str, Any]] = []
    for batch in batches:
        for url in batch["url"]:
            for row in crawl_archive(BlobURL(url)):
                rows.append(row)
                if len(rows) >= FLUSH_EVERY:
                    yield pandas.DataFrame(rows)
                    rows = []
    if rows:
        yield pandas.DataFrame(rows)


def crawl_archives_for_date(
    spark: SparkSession,
    archive_date: date,
    archives: List[BlobURL],
) -> DataFrame:
    table_name = archive_date.isoformat().replace("-", "_")
    if spark.catalog.tableExists(table_name):
        return spark.table(table_name)

    # TODO: Parallelizing on XML files (inside the zips) instead of just the zips
    # would speed up dataframe building for days that only have few archives a lot.
    # One partition per archive so each task processes exactly one zip — matches the
    # behavior of the previous sparkContext.parallelize(archives) path.
    urls_df = spark.createDataFrame(
        [(str(a),) for a in archives], schema="url string"
    ).repartition(len(archives), "url")
    df = urls_df.mapInPandas(_parse_archives_udf, schema=company_record_schema).persist(
        StorageLevel.DISK_ONLY
    )
    # Not historical merging across dumps (that happens a layer above) — just collapsing
    # duplicate IDs within this single dump, which appears to be a bureaucratic artifact.
    df = merge_duplicate_company_records(df)

    df.write.saveAsTable(table_name, mode="overwrite")

    return spark.table(table_name)
