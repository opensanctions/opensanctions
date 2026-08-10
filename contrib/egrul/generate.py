from collections import defaultdict
from datetime import date, datetime
from functools import reduce
import os
from pathlib import Path
import tempfile
from typing import Generator, List, Iterable, Dict
from zipfile import ZipFile
from dataclasses import dataclass

import numpy as np
import pandas
from pyspark import StorageLevel
from pyspark.sql import Column, SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    array_distinct,
    array_sort,
    collect_list,
    concat,
    concat_ws,
    date_sub,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    transform,
    when,
    col,
    coalesce,
    explode_outer,
    explode,
)
from google.cloud.storage import Client  # type: ignore

from egrul_xml import parse_xml
from parse_context import ParseContext
from schema import company_record_schema
from zavod import Context
from zavod import Dataset

LOCAL_BUCKET_CACHE_DIR = Path(
    os.environ.get("LOCAL_BUCKET_CACHE_DIR", tempfile.gettempdir())
)
SOURCE_DATA_BUCKET_NAME = "egrul.opensanctions.org"
PROCESSESED_PREFIX = "ru_egrul/processed/"

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


def merge_duplicate_company_records(df: DataFrame) -> DataFrame:
    """Deduplicate companies that have the same ID in the given DataFrame."""

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


def get_local_archive_path(blob_url: BlobURL) -> Path:
    return LOCAL_BUCKET_CACHE_DIR / str(blob_url.name)


def crawl_archive(blob_url: BlobURL) -> Generator[dict, None, None]:
    data_date = get_archive_date_from_blob_url(blob_url)
    context = get_context()

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
                pc = ParseContext(
                    origin="%s/%s" % (Path(blob_url.name).name, name),
                    data_time=data_date,
                    _context=context,
                )
                with zip.open(name, "r") as fh:
                    for e in parse_xml(pc, fh):
                        yield e
    finally:
        # Don't clean up the temporary file, for now this is being run on Leon's machine and it's
        # okay to just have them cached.
        # os.unlink(local_archive_path)
        pass


def archive_table_name(archive_date: date) -> str:
    return archive_date.isoformat().replace("-", "_")


def crawl_archives_for_date(
    spark: SparkSession,
    archive_date: date,
    archives: List[BlobURL],
) -> DataFrame:
    table_name = archive_table_name(archive_date)
    if spark.catalog.tableExists(table_name):
        return spark.table(table_name)

    # TODO: Parallelizing on XML files (inside the zips) instead of just the zips
    # would speed up dataframe building for days that only have few archives a lot.
    blob_rdd = spark.sparkContext.parallelize(archives)
    parsed_rows_rdd = blob_rdd.flatMap(crawl_archive)
    # Persist this expensive computation to avoid doing it multiple times during the following join
    # https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose
    df = spark.createDataFrame(parsed_rows_rdd, schema=company_record_schema).persist(
        StorageLevel.DISK_ONLY
    )
    df = merge_duplicate_company_records(df)

    df.write.saveAsTable(table_name, mode="overwrite")
    df.unpersist()

    return spark.table(table_name)


# === Assembling current state out of the archives ===
#
# Every archive is a set of company records, each listing that company's ownerships and
# directorships at that point in time. A yearly FULL archive lists every company; a daily
# archive lists only the companies that changed that day. So:
#
#   - A company missing from an archive means nothing at all.
#   - An ownership or directorship missing from a record that *is* in the archive has
#     ended: the registry described the company and didn't mention it.
#
# That makes "when did this end?" a question about the company's own appearances, never
# about other companies. We therefore don't build current state by folding archives into
# each other; we group by company instead. Each ownership and directorship gets one
# "tenure" per run of consecutive appearances of its company that list it, and the
# company's next appearance after a tenure is the archive that ended it.
#
# The grouping runs over "appearance" tables a few strings wide, so the nested records are
# only touched again at the end, to pick up the winning version of each relationship.
#
# All of these tables are rebuilt on every run, unlike the per-archive tables. They're
# derived from the whole set of archives, so as soon as one more archive exists they are
# stale, and a new archive can end a relationship belonging to any company anywhere in
# history. Reusing them would silently omit whatever arrived since.

# The nested relationship arrays inside a company record, both handled identically.
RELATIONSHIP_ARRAY_COLUMNS = ("ownerships", "directorships")

COMPANY_APPEARANCES_TABLE = "company_appearances"
RELATIONSHIP_APPEARANCES_TABLE = "relationship_appearances"
DIRECTORSHIP_SUCCESSOR_STARTS_TABLE = "directorship_successor_starts"
RELATIONSHIP_TENURES_TABLE = "relationship_tenures"

# Every stage after the appearance tables joins, windows or groups by company id, so the
# tables are bucketed on it and Spark can do all of that without shuffling.
NUM_BUCKETS = 200


def write_company_bucketed_table(
    df: DataFrame, table_name: str, sort_by: List[str]
) -> None:
    """Write a table bucketed and sorted by company id.

    The repartition uses the same hash as Spark's bucketing, so each task writes exactly
    one bucket; without it every task writes a file into every bucket.
    """
    (
        df.repartition(NUM_BUCKETS, col("id"))
        .write.mode("overwrite")
        .bucketBy(NUM_BUCKETS, "id")
        .sortBy(*sort_by)
        .saveAsTable(table_name)
    )


def read_archive_records(
    spark: SparkSession, archive_dates: List[date], columns: List[str]
) -> DataFrame:
    """Read the per-archive-date tables as one DataFrame tagged with `archive_date`.

    Deliberately lazy and column-projected: callers ask for the few columns they need so
    Spark's Parquet reader skips the rest. That matters a lot, because the nested
    ownership and directorship arrays dwarf everything else in a record.
    """
    return reduce(
        DataFrame.unionByName,
        [
            spark.table(archive_table_name(d))
            .select(*columns)
            .withColumn("archive_date", lit(d))
            for d in archive_dates
        ],
    )


def build_appearance_tables(
    spark: SparkSession, archive_dates: List[date]
) -> tuple[DataFrame, DataFrame]:
    """Materialise which companies, ownerships and directorships each archive listed.

    This is the only place the full set of records is scanned for the end-date logic, and
    it reduces them to a few skinny columns that everything downstream groups by.
    """
    # One row per (company, archive date) already, because the per-date tables are
    # deduplicated by company id. The company's own origin is kept here so that when this
    # archive ends a relationship, we can name it as the source of that end date.
    companies = read_archive_records(
        spark, archive_dates, ["id", "legal_entity.origin"]
    )
    write_company_bucketed_table(
        companies, COMPANY_APPEARANCES_TABLE, ["id", "archive_date"]
    )

    relationships = reduce(
        DataFrame.unionByName,
        [
            read_archive_records(spark, archive_dates, ["id", kind]).select(
                "id",
                lit(kind).alias("kind"),
                # A record can list the same relationship twice, and the window functions
                # downstream need one row per appearance. Deduplicating within the array
                # does that without a shuffle, and it's enough: only ids from the same
                # array can collide, because the per-date tables are already deduplicated
                # by company.
                explode(array_distinct(transform(col(kind), lambda r: r["id"]))).alias(
                    "relationship_id"
                ),
                "archive_date",
            )
            for kind in RELATIONSHIP_ARRAY_COLUMNS
        ],
    )
    write_company_bucketed_table(
        relationships, RELATIONSHIP_APPEARANCES_TABLE, ["id", "archive_date"]
    )

    return (
        spark.table(COMPANY_APPEARANCES_TABLE),
        spark.table(RELATIONSHIP_APPEARANCES_TABLE),
    )


def build_successor_start_table(
    spark: SparkSession, archive_dates: List[date]
) -> DataFrame:
    """Collect the earliest directorship start date per company, archive and role.

    Where the archive that dropped a directorship names a successor in the same role, that
    successor's start date is a sharper end date than the archive date: the registry's own
    record date can predate the archive that publishes it.
    """
    starts = (
        read_archive_records(spark, archive_dates, ["id", "directorships"])
        .select(
            "id",
            "archive_date",
            explode(col("directorships")).alias("directorship"),
        )
        .groupBy("id", "archive_date", col("directorship.role").alias("role"))
        .agg(spark_min("directorship.start_date").alias("successor_start_date"))
    )
    write_company_bucketed_table(
        starts, DIRECTORSHIP_SUCCESSOR_STARTS_TABLE, ["id", "archive_date"]
    )

    return spark.table(DIRECTORSHIP_SUCCESSOR_STARTS_TABLE)


def number_company_appearances(company_appearances: DataFrame) -> DataFrame:
    """Number each company's own appearances consecutively, oldest first.

    Absence from an archive is only evidence about a company that is in that archive, so
    all our reasoning is against the company's own list of appearances rather than the
    calendar. This sequence number is what makes "consecutive" and "the next one" mean
    that.
    """
    return company_appearances.select("id", "archive_date").withColumn(
        "seq", row_number().over(Window.partitionBy("id").orderBy("archive_date"))
    )


def build_relationship_tenures(
    spark: SparkSession,
    company_appearances: DataFrame,
    relationship_appearances: DataFrame,
) -> DataFrame:
    """Find each relationship's runs of consecutive appearances and what ended each.

    A relationship that disappears and comes back later gets one tenure per run, so a
    directorship held twice reads as two directorships rather than one long one.

    The closing archive is null for the tenure that is still listed in the company's
    latest record: that relationship is current, and gets no end date.
    """
    appearances = number_company_appearances(company_appearances)
    dated = relationship_appearances.join(
        appearances, on=["id", "archive_date"], how="inner"
    )

    # Across a run of consecutive seq values, seq minus a counter over the same rows
    # stays constant, which is what identifies the run.
    tenure_id = col("seq") - row_number().over(
        Window.partitionBy("id", "kind", "relationship_id").orderBy("seq")
    )
    tenures = (
        dated.withColumn("tenure_id", tenure_id)
        .groupBy("id", "kind", "relationship_id", "tenure_id")
        .agg(spark_max("seq").alias("last_seq"))
    )

    last_seen = appearances.select(
        "id",
        col("seq").alias("last_seq"),
        col("archive_date").alias("last_archive_date"),
    )
    closing = appearances.select(
        "id",
        (col("seq") - 1).alias("last_seq"),
        col("archive_date").alias("closing_archive_date"),
    )
    resolved = (
        tenures.join(last_seen, on=["id", "last_seq"], how="inner")
        .join(closing, on=["id", "last_seq"], how="left")
        .select(
            "id",
            "kind",
            "relationship_id",
            "last_archive_date",
            "closing_archive_date",
        )
    )
    write_company_bucketed_table(resolved, RELATIONSHIP_TENURES_TABLE, ["id"])

    return spark.table(RELATIONSHIP_TENURES_TABLE)


def join_winning_relationship_versions(
    spark: SparkSession,
    archive_dates: List[date],
    tenures: DataFrame,
    company_appearances: DataFrame,
    kind: str,
) -> DataFrame:
    """Attach each tenure to the relationship as its last archive described it.

    The last archive listing a relationship holds the registry's final word on it, so
    that's the version we keep. This is the one join that touches the nested records, and
    it's keyed by company, as is the aggregation that follows.
    """
    relationships = (
        read_archive_records(spark, archive_dates, ["id", kind])
        .select("id", "archive_date", explode(col(kind)).alias("relationship"))
        .withColumn("relationship_id", col("relationship.id"))
    )
    ends = tenures.where(col("kind") == kind).select(
        "id",
        "relationship_id",
        col("last_archive_date").alias("archive_date"),
        "closing_archive_date",
    )
    closing_origins = company_appearances.select(
        "id",
        col("archive_date").alias("closing_archive_date"),
        col("origin").alias("closing_origin"),
    )
    return relationships.join(
        ends, on=["id", "archive_date", "relationship_id"], how="inner"
    ).join(
        # Left join: a tenure with no closing archive is still current.
        closing_origins,
        on=["id", "closing_archive_date"],
        how="left",
    )


def collect_resolved_relationships(
    tenures: DataFrame, end_date: Column, output_column: str
) -> DataFrame:
    """Stamp end date and closing origin onto each relationship, then group by company.

    A relationship the source itself gave an end date keeps that date, and keeps its
    origin untouched: we only claim the closing archive as provenance for end dates we
    derived from it.
    """
    ends_here = (
        col("relationship.end_date").isNull() & col("closing_archive_date").isNotNull()
    )
    resolved = (
        col("relationship")
        .withField(
            "end_date",
            when(ends_here, end_date).otherwise(col("relationship.end_date")),
        )
        .withField(
            "origin",
            when(
                ends_here, concat(col("relationship.origin"), col("closing_origin"))
            ).otherwise(col("relationship.origin")),
        )
    )
    return (
        tenures.select("id", resolved.alias("relationship"))
        .groupBy("id")
        .agg(collect_list("relationship").alias(output_column))
    )


def resolve_ownerships(
    spark: SparkSession,
    archive_dates: List[date],
    tenures: DataFrame,
    company_appearances: DataFrame,
) -> DataFrame:
    """Build the final ownerships array per company."""
    ownerships = join_winning_relationship_versions(
        spark, archive_dates, tenures, company_appearances, "ownerships"
    )
    # The company was described without this ownership on the closing date, so the last
    # day it can have held is the day before.
    return collect_resolved_relationships(
        ownerships, date_sub(col("closing_archive_date"), 1), "ownerships"
    )


def resolve_directorships(
    spark: SparkSession,
    archive_dates: List[date],
    tenures: DataFrame,
    company_appearances: DataFrame,
    successor_starts: DataFrame,
) -> DataFrame:
    """Build the final directorships array per company, ending each at its successor."""
    directorships = join_winning_relationship_versions(
        spark, archive_dates, tenures, company_appearances, "directorships"
    )
    successor_starts = successor_starts.select(
        "id",
        col("archive_date").alias("closing_archive_date"),
        "role",
        "successor_start_date",
    )
    directorships = (
        directorships.alias("tenure")
        .join(
            successor_starts.alias("successor"),
            on=[
                col("tenure.id") == col("successor.id"),
                col("tenure.closing_archive_date")
                == col("successor.closing_archive_date"),
                # Null-safe so that untitled directorships match each other rather than
                # dropping out of the comparison.
                col("tenure.relationship.role").eqNullSafe(col("successor.role")),
            ],
            how="left",
        )
        .select(
            col("tenure.id").alias("id"),
            col("tenure.relationship").alias("relationship"),
            col("tenure.archive_date").alias("last_archive_date"),
            col("tenure.closing_archive_date").alias("closing_archive_date"),
            col("tenure.closing_origin").alias("closing_origin"),
            col("successor.successor_start_date").alias("successor_start_date"),
        )
    )
    # Only trust the successor's start date if it falls after we last saw this
    # directorship; otherwise the registry is describing something we can't reconcile and
    # the archive date is the only date we can defend.
    successor_end_date = when(
        col("successor_start_date") > col("last_archive_date"),
        date_sub(col("successor_start_date"), 1),
    )
    return collect_resolved_relationships(
        directorships,
        coalesce(successor_end_date, date_sub(col("closing_archive_date"), 1)),
        "directorships",
    )


def assemble_company_records(
    spark: SparkSession,
    archive_dates: List[date],
    company_appearances: DataFrame,
    ownerships: DataFrame,
    directorships: DataFrame,
) -> DataFrame:
    """Combine each company's latest record with its resolved ownerships and directorships.

    Everything that isn't an ownership or directorship (the company itself, its
    successions) is simply taken from the latest record we have for the company; the
    registry restates all of it on every appearance.

    Companies with no ownerships or directorships at all get a null array rather than an
    empty one, which the CSV writer's explode treats the same way.
    """
    latest = company_appearances.groupBy("id").agg(
        spark_max("archive_date").alias("archive_date")
    )
    return (
        read_archive_records(
            spark, archive_dates, ["id", "legal_entity", "successions"]
        )
        .join(latest, on=["id", "archive_date"], how="inner")
        .drop("archive_date")
        .join(ownerships, on="id", how="left")
        .join(directorships, on="id", how="left")
        .select("id", "legal_entity", "successions", "ownerships", "directorships")
    )


def flatten_origin(df: DataFrame) -> DataFrame:
    """Join the origin array into one comma-separated field.

    Origins never contain a comma, and the CSV writer can't write arrays anyway.
    Sorting and deduplicating keeps the output stable across runs.
    """
    return df.withColumn(
        "origin", concat_ws(",", array_sort(array_distinct(col("origin"))))
    )


def write_companies_df_to_csv(df: DataFrame, path_prefix: Path) -> None:
    """Write the companies DataFrame to CSV files to be emitted as FtM in the ru_egrul crawler.

    The processing happening here is basically:
      - Explode some arrays, emitting the Entity to the CSV multiple times

      - Retrieve owners, directors, and successor companies from inside the nested structure and append them to
        the full Person/LegalEntity CSV table."""
    companies_df = df.select("legal_entity.*")

    ownerships_df = df.withColumn("ownership", explode(col("ownerships"))).select(
        "ownership.*"
    )

    # Keep owners around for later before we drop it from the ownerships table
    owners_df = ownerships_df.select("owner.*")
    ownerships_df = ownerships_df.withColumn(
        "owner_id", coalesce(col("owner.person.id"), col("owner.legal_entity.id"))
    ).drop("owner")

    directorships_df = df.withColumn(
        "directorship", explode(col("directorships"))
    ).select("directorship.*")
    # Keep directors around for later before we drop it from the directorships table
    directors_df = directorships_df.select("director.*")
    #
    directorships_df = directorships_df.withColumn(
        "director_id", col("director.id")
    ).drop("director")

    successions_df = df.withColumn("succession", explode(col("successions"))).select(
        "succession.*"
    )

    # Get the successor and predecessor companies, we want to add them to the main company table
    successor_companies_df = successions_df.where(col("successor").isNotNull()).select(
        "successor.*"
    )
    predecessor_companies_df = successions_df.where(
        col("predecessor").isNotNull()
    ).select("predecessor.*")

    # Drop the successor and predecessor columns from the successions table, we still have the relationships
    # in the successor_id and predecessor_id columns.
    successions_df = successions_df.drop("successor").drop("predecessor")

    # Owner can be either a Person or a LegalEntity, split up the union type
    owners_person_df = owners_df.where(col("person").isNotNull()).select("person.*")
    owners_legalentity_df = owners_df.where(col("legal_entity").isNotNull()).select(
        "legal_entity.*"
    )

    # Both directors and owners can be persons, we want to emit them to the same table
    persons_df = owners_person_df.union(directors_df)
    # We use explode_outer here because we want to keep Person records without any countries
    persons_df = persons_df.withColumn("country", explode_outer(col("countries"))).drop(
        "countries"
    )

    # Join companies at the root and in the successors and predecessors
    # and explode their addresses array.
    all_legal_entities_df = (
        companies_df.union(successor_companies_df)
        .union(predecessor_companies_df)
        .union(owners_legalentity_df)
    )
    # We use explode_outer here beceause we want to keep records without any addresses
    all_legal_entities_df = all_legal_entities_df.withColumn(
        "address", explode_outer(col("addresses"))
    ).drop("addresses")

    # This is what's required for the Python csv module to read the file with no further options
    csv_options = {"header": True, "escape": '"', "mode": "overwrite"}
    flatten_origin(ownerships_df).write.csv(
        str(path_prefix / "ownerships"), **csv_options
    )
    flatten_origin(directorships_df).write.csv(
        str(path_prefix / "directorships"), **csv_options
    )
    flatten_origin(successions_df).write.csv(
        str(path_prefix / "successions"), **csv_options
    )
    flatten_origin(persons_df).write.csv(str(path_prefix / "persons"), **csv_options)
    flatten_origin(all_legal_entities_df).write.csv(
        str(path_prefix / "legalentities"), **csv_options
    )


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


def crawl(context: Context) -> None:
    # .enableHiveSupport() is required to use tables in spark.catalog
    spark = SparkSession.builder.appName("ru_egrul").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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

    archives_by_date = sorted(aggregate_archives_by_date(archives).items())
    archives_by_date = [
        (d, archives)
        for d, archives in archives_by_date
        # For debugging (or manual partial resume), process only part of the data
        # if date(2022, 1, 1) <= d <= date(2022, 12, 31)
        # Take 2022-01-01 as the starting point
        if date(2022, 1, 1) <= d
    ]

    # Parse every archive into its own table first. These are the only cached step: an
    # archive's contents never change, so parsing can be interrupted and resumed.
    for archive_date, archives in archives_by_date:
        context.log.info("Processing %s" % archive_date)
        crawl_archives_for_date(spark, archive_date, archives)

    archive_dates = [archive_date for archive_date, _ in archives_by_date]

    context.log.info("Collecting company and relationship appearances")
    company_appearances, relationship_appearances = build_appearance_tables(
        spark, archive_dates
    )

    successor_starts = build_successor_start_table(spark, archive_dates)

    context.log.info("Finding ownership and directorship tenures")
    tenures = build_relationship_tenures(
        spark, company_appearances, relationship_appearances
    )

    context.log.info("Resolving current ownerships and directorships")
    ownerships = resolve_ownerships(spark, archive_dates, tenures, company_appearances)
    directorships = resolve_directorships(
        spark, archive_dates, tenures, company_appearances, successor_starts
    )
    final_df = assemble_company_records(
        spark, archive_dates, company_appearances, ownerships, directorships
    )

    last_date = archive_dates[-1]
    final_table_name = "current_" + last_date.isoformat().replace("-", "_")
    final_df.write.saveAsTable(final_table_name, mode="overwrite")

    # Read the result back rather than reusing final_df, which would compute the whole
    # pipeline a second time to write the CSVs.
    write_companies_df_to_csv(
        spark.table(final_table_name),
        LOCAL_BUCKET_CACHE_DIR / PROCESSESED_PREFIX / final_table_name,
    )


def get_context() -> Context:
    dataset = Dataset.from_path("datasets/ru/egrul/ru_egrul.yml")
    return Context(dataset)


def main() -> None:
    context = get_context()
    crawl(context)


if __name__ == "__main__":
    main()
