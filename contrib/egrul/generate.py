from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Iterable, Dict
from zipfile import ZipFile

import numpy as np
import pandas
from pyspark import Row, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.functions import (
    struct,
    udf,
    col,
    coalesce,
    explode_outer,
    explode,
)

from egrul_xml import parse_xml
from schema import company_record_schema
from zavod import Context
from zavod import Dataset

LOCAL_BUCKET_PATH = "/Users/leon/internal-data/"
# _406 denotes the format version
INTERNAL_DATA_ARCHIVE_PREFIX = "ru_egrul/egrul.itsoft.ru/EGRUL_406/"
INTERNAL_DATA_CACHE_PREFIX = "ru_egrul/cache"
INTERNAL_DATA_PROCESSESED_PREFIX = "ru_egrul/processed/"


def day_before(d: date) -> date:
    return d - timedelta(days=1)


def update_company_from_new_company(context: Context, old: Row, new: Row) -> Row:
    """Enriches a company with information from a previous version."""
    # Doing this in Python is too expensive, we use a Spark join
    assert old is not None and new is not None, "Both old and new must be present"

    result = new
    data_date = new.legal_entity.seen_date

    expired_ownerships = []
    new_ownership_ids = set([o.id for o in result.ownerships])
    for o in old.ownerships:
        # Ownership.id is built from (owner, asset, shares_count, role), so if any of these changes,
        # we'll add an expired Ownership
        if o.id not in new_ownership_ids:
            # If we already have an end date, use that. It might have been set by
            # a previous run of this function when the new directorship first appeared.
            if o.end_date:
                expired_ownerships.append(o)
            else:
                o_dict = o.asDict()
                o_dict["end_date"] = day_before(data_date)
                expired_ownerships.append(Row(**o_dict))

    expired_directorships = []
    new_directorship_ids = set([o.id for o in result.directorships])
    for d in old.directorships:
        # Directorship.id is built from (company, director, role), so if any of these changes,
        # we'll add an expired Directorship
        if d.id not in new_directorship_ids:
            # If we already have an end date, use that. It might have been set by
            # a previous run of this function when the new directorship first appeared.
            if d.end_date:
                expired_directorships.append(d)
            else:
                # We try to find a new directorship with the same role, and if it has a start_date,
                # we use that as a better end_date for the previous directorship than just the day we saw it.
                new_directorship_same_role = [
                    new_d for new_d in result.directorships if new_d.role == d.role
                ]
                d_dict = d.asDict()
                if (
                    len(new_directorship_same_role) > 0
                    and new_directorship_same_role[0].start_date is not None
                ):
                    d_dict["end_date"] = day_before(
                        new_directorship_same_role[0].start_date
                    )
                else:
                    d_dict["end_date"] = day_before(data_date)
                expired_directorships.append(Row(**d_dict))

    if expired_directorships or expired_ownerships:
        result_dict = result.asDict()
        # context.log.info(
        #     "Adding %d ended ownerships and %d ended directorships to %s"
        #     % (len(expired_ownerships), len(expired_directorships), result_dict["id"])
        # )
        result_dict["ownerships"].extend(expired_ownerships)
        result_dict["directorships"].extend(expired_directorships)
        result = Row(**result_dict)
    return result


def update_companies_from_new_companies(
    context: Context, old: DataFrame, new: DataFrame
) -> DataFrame:
    """Update a dataframe of companies with information from a new version."""
    context.log.info("Merging %d new" % new.count())
    old = old.alias("old")
    new = new.alias("new")

    # We run the python code only on rows where both old and new are present because it's expensive
    to_merge = old.join(new, on="id", how="inner")

    merge_fn = udf(
        lambda old, new: update_company_from_new_company(context, old, new),
        company_record_schema,
    )
    merged = (
        to_merge.withColumn(
            "merged", merge_fn(struct(col("old.*")), struct(col("new.*")))
        )
        # The select at the end "flattens" the company struct back into the root of the dataframe
        # (instead of being nested in a "new", "old" or "merged" column)
        .select("merged.*")
    )

    not_to_merge_old = old.join(new, on="id", how="left_anti")
    not_to_merge_new = new.join(old, on="id", how="left_anti")

    return merged.union(not_to_merge_old).union(not_to_merge_new)


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


def crawl_local_archive(zip_path: str):
    data_date = get_archive_date(Path(zip_path))
    context = get_context(data_date)

    context.log.info("Opening archive: %s" % zip_path)

    with ZipFile(zip_path, "r") as zip:
        for name in zip.namelist():
            if not name.lower().endswith(".xml"):
                continue
            with zip.open(name, "r") as fh:
                for e in parse_xml(context, fh):
                    yield e


def crawl_archives_for_date(
    spark: SparkSession,
    archive_date: date,
    archives: List[Path],
):
    table_name = archive_date.isoformat().replace("-", "_")
    if spark.catalog.tableExists(table_name):
        return spark.table(table_name)

    # TODO: Parallelizing on XML files (inside the zips) instead of just the zips
    # would speed up dataframe building for days that only have few archives a lot.
    path_rdd = spark.sparkContext.parallelize(archives)
    parsed_rows_rdd = path_rdd.flatMap(crawl_local_archive)
    # Persist this expensive computation to avoid doing it multiple times during the following join
    # https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose
    df = spark.createDataFrame(parsed_rows_rdd, schema=company_record_schema).persist(
        StorageLevel.DISK_ONLY
    )
    df = merge_duplicate_company_records(df)

    df.write.saveAsTable(table_name, mode="overwrite")

    return spark.table(table_name)


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
    ownerships_df.write.csv(str(path_prefix / "ownerships"), **csv_options)
    directorships_df.write.csv(str(path_prefix / "directorships"), **csv_options)
    successions_df.write.csv(str(path_prefix / "successions"), **csv_options)
    persons_df.write.csv(str(path_prefix / "persons"), **csv_options)
    all_legal_entities_df.write.csv(str(path_prefix / "legalentities"), **csv_options)


def get_archive_date(archive_path: Path) -> date:
    """Gets an archive date from the archive path."""
    dirname = archive_path.parts[-2]  # [..., "dirname", "archive.zip"]
    dirname = dirname.rstrip("_FULL")
    return datetime.strptime(dirname, "%d.%m.%Y").date()


def aggregate_archives_by_date(
    archive_paths: Iterable[Path],
) -> Dict[date, Iterable[Path]]:
    archives_by_date = defaultdict(set)
    for archive_path in archive_paths:
        archive_date = get_archive_date(archive_path)
        archives_by_date[archive_date].add(archive_path)
    return archives_by_date


def merge_company_record_dfs(
    context: Context, records: Iterable[DataFrame]
) -> DataFrame:
    """Merge company records using update_companies_from_new_companies."""
    result = None
    for record in records:
        # First iteration, set result to the first crawled DataFrame
        if result is None:
            result = record
            continue

        result = update_companies_from_new_companies(context, result, record)
        # Reduce number of partitions, otherwise it will explode with every iteration (don't ask me why)
        # and eventually grind everything to a halt.
        result = result.coalesce(16)
        # Checkpoint cuts the lineage of the DataFrame, so we don't
        # end up with a huge execution plan
        result = result.checkpoint()

        context.log.info("Updated state has %d company records" % result.count())

    return result


def crawl(context: Context) -> None:
    # .enableHiveSupport() is required to use tables in spark.catalog
    spark = SparkSession.builder.appName("ru_egrul").enableHiveSupport().getOrCreate()
    spark.sparkContext.setCheckpointDir("env/spark-checkpoint")
    spark.sparkContext.setLogLevel("WARN")

    # TODO(Leon Handreke): Rewrite to ingest from and output to Google Storage.
    archives = [
        name
        for name in Path(LOCAL_BUCKET_PATH)
        .joinpath(INTERNAL_DATA_ARCHIVE_PREFIX)
        .glob("**/*.zip")
    ]

    archives_by_date = sorted(aggregate_archives_by_date(archives).items())
    archives_by_date = [
        (d, archives)
        for d, archives in archives_by_date
        # For debugging (or manual partial resume), process only part of the data
        # if date(2022, 1, 1) <= d <= date(2022, 12, 31)
        # Take 2022-01-01 as the starting point
        if date(2022, 1, 1) <= d
    ]

    # Process the archives first to have them ready in the warehouse for the
    # iterative join later.
    for archive_date, archives in archives_by_date:
        context.log.info("Processing %s" % archive_date)
        crawl_archives_for_date(spark, archive_date, archives)

    # The general idea is that we continously fold new data into the existing data, starting at the full dump
    # at 2022-01-01. Because updating the full dataset with every new incremental update takes too long
    # (~90s per join on my M4, depending a bit on the size of the new data), we exploit the fact that the operation is
    # associative (but it's not commutative!). So, we first merge all the months, then fold these into one
    # yearly partial update, and finally merge that into the full dump, to get the state at the end of the year.
    # We do this for every year and then merge the years. We could also do chunks of 20 or whatever, it doesn't matter.
    # What does matter is to reduce the number of joins with very large datasets.
    # Saving to partial_ tables is not strictly required, but quite useful to interrupt and resume the process when
    # running locally and running sql queries on it.
    year_dfs = []
    for year in [2022, 2023, 2024, 2025]:
        year_archives = [
            (archive_date, archives)
            for archive_date, archives in archives_by_date
            if archive_date.year == year
        ]
        full_archive = year_archives[0]
        partial_archives = year_archives[1:]

        year_table_name = "partial_%d" % year
        if spark.catalog.tableExists(year_table_name):
            context.log.info("Reading %d from table" % year)
            year_dfs.append(spark.table(year_table_name))
            continue

        month_dfs = []
        for month in range(1, 13):
            month_table_name = "partial_%d_%02d" % (year, month)
            if spark.catalog.tableExists(month_table_name):
                context.log.info("Reading %d-%d from table" % (year, month))
                month_dfs.append(spark.table(month_table_name))
                continue

            context.log.info("Processing %d-%d" % (year, month))
            day_archives = [
                x for x in partial_archives if x[0].year == year and x[0].month == month
            ]
            day_dfs = [crawl_archives_for_date(spark, x[0], x[1]) for x in day_archives]
            context.log.info("Merging day records for %d-%d" % (year, month))
            if len(day_dfs) == 0:
                continue

            month_df = merge_company_record_dfs(context, day_dfs)
            context.log.info("%d-%d has %d records" % (year, month, month_df.count()))

            month_df.write.saveAsTable(month_table_name)
            month_df = month_df.persist(StorageLevel.DISK_ONLY)
            month_dfs.append(month_df)

        context.log.info("Merging partial monthly records for %d" % year)
        partial_year_df = merge_company_record_dfs(context, month_dfs)
        full_df = crawl_archives_for_date(spark, full_archive[0], full_archive[1])
        context.log.info("Merging full and partial daily records for %d" % year)
        year_df = merge_company_record_dfs(context, [full_df, partial_year_df])
        context.log.info("%d has %d records" % (year, year_df.count()))

        # Persist to make sure that Spark doesn't get the idea to throw away and recompute this.
        year_df = year_df.persist(StorageLevel.DISK_ONLY)
        year_df.write.saveAsTable(year_table_name, mode="overwrite")
        year_dfs.append(year_df)

    context.log.info("Merging yearly records")
    # Each of these (join of ~12M on ~12M records with merge UDF in Python) takes around 8min on M4 10 cores
    final_df = merge_company_record_dfs(context, year_dfs)

    last_date = archives_by_date[-1][0]
    final_table_name = "current_" + last_date.isoformat().replace("-", "_")
    final_df.write.saveAsTable(final_table_name)

    write_companies_df_to_csv(
        final_df,
        Path(LOCAL_BUCKET_PATH, INTERNAL_DATA_PROCESSESED_PREFIX, final_table_name),
    )


def get_context(data_time: Optional[date] = None) -> Context:
    dataset = Dataset.from_path("datasets/ru/egrul/ru_egrul.yml")
    context = Context(dataset)
    if data_time is not None:
        context._data_time = datetime.combine(data_time, datetime.min.time())
    return context


def main():
    context = get_context()
    crawl(context)


if __name__ == "__main__":
    main()
