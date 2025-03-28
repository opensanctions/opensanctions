from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Iterable, Dict
from zipfile import ZipFile

import numpy as np
import pandas
from pyspark import Row
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
INTERNAL_DATA_ARCHIVE_PREFIX = "ru_egrul/egrul.itsoft.ru/EGRUL_406/"
INTERNAL_DATA_CACHE_PREFIX = "ru_egrul/cache"
INTERNAL_DATA_PROCESSESED_PREFIX = "ru_egrul/processed/"


def day_before(d: date) -> date:
    return d - timedelta(days=1)


@udf(company_record_schema)
def update_company_from_new_company(context: Context, old: Row, new: Row) -> Row:
    """Enriches a company with information from a previous version."""
    # Doing this in Python is too expensive, we use a Spark join
    assert old is not None and new is not None, "Both old and new must be present"

    result = new
    data_date = new.seen_date

    assert all(
        [o.start_date is not None for o in result.ownerships]
    ), "Ownership does not have a start_date"

    expired_ownerships = []
    new_ownership_ids = set([o.id for o in result.ownerships])
    for o in old.ownerships:
        # Ownership.id is built from (owner, asset, shares_count, role), so any change will build a new Ownership
        if o.id not in new_ownership_ids:
            o_dict = o.asDict()
            o_dict["end_date"] = day_before(data_date)
            expired_ownerships.append(Row(**o_dict))

    expired_directorships = []
    new_directorship_ids = set([o.id for o in result.directorships])
    for d in old.directorships:
        # Directorship.id is built from (company, director, role)
        if d.id not in new_directorship_ids:
            # We try to find a new directorship with the same role, and if it has a start_date,
            # we use that as a better end_date for the previous directorship than just the day we saw it.
            new_directorship_same_role = [
                new_d for new_d in result.directorships if new_d.role == d.id
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
        context.log.info(
            "Adding %d ended ownerships and %d ended directorships to %s"
            % (len(expired_ownerships), len(expired_directorships), result_dict["id"])
        )
        result_dict["ownerships"].extend(expired_ownerships)
        result_dict["directorships"].extend(expired_directorships)
        result = Row(**result_dict)
    return result


def update_companies_from_new_companies(
    context: Context, old: DataFrame, new: DataFrame
) -> DataFrame:
    """Update a dataframe of companies with information from a new version."""
    context.log.info("Merging %d old and %d new" % (old.count(), new.count()))
    # Nest the dataframes in "new" and "old" column so they're easier to work with later
    old_nested = old.select(struct(*old.columns).alias("old"))
    new_nested = new.select(struct(*new.columns).alias("new"))

    joined = old_nested.join(new_nested, col("old.id") == col("new.id"), "outer")

    # We run the python code only on rows where both old and new are present because it's expensive
    to_merge = joined.where(joined.old.isNotNull() & joined.new.isNotNull())
    context.log.info("to_merge %d" % to_merge.count())

    # The select at the end "flattens" the company struct back into the root of the dataframe
    # (instead of being nested in a "new", "old" or "merged" column)
    merged = to_merge.withColumn(
        "merged", update_company_from_new_company(col("old"), col("new"))
    ).select("merged.*")
    not_to_merge_old = joined.where(col("new.id").isNull()).select("old.*")
    not_to_merge_new = joined.where(col("old.id").isNull()).select("new.*")
    return merged.union(not_to_merge_old).union(not_to_merge_new)


def merge_duplicate_company_records(df: DataFrame) -> DataFrame:
    """Merge companies that have the same in the given DataFrame."""

    dupe_ids = df.groupBy("id").count().where(col("count") > 1).drop("count")
    dupes = df.join(dupe_ids, on="id", how="inner")
    non_dupes = df.join(dupe_ids, on="id", how="left_anti")

    def _merge_companies(pdf: pandas.DataFrame) -> pandas.DataFrame:
        # TODO(Leon Handreke): Implement a more sophisticated merge strategy. For now, all of them
        # seem to be dissolved and reopened companies, some bureaucratic artifact.
        # active = pdf[pdf["dissolution_date"].isnull()]
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
    df_path = Path(
        LOCAL_BUCKET_PATH,
        INTERNAL_DATA_CACHE_PREFIX,
        archive_date.isoformat() + ".parquet",
    )
    if df_path.exists():
        df = spark.read.parquet(str(df_path))
    else:
        path_rdd = spark.sparkContext.parallelize(archives)
        # The .cache() ensures that we won't accidentally run this multiple times just because the
        # execution engine thinks it's a good idea. That's just wasteful.
        parsed_rows_rdd = path_rdd.flatMap(crawl_local_archive).cache()
        df = spark.createDataFrame(parsed_rows_rdd, schema=company_record_schema)
        df = merge_duplicate_company_records(df)
        df.write.mode("overwrite").parquet(str(df_path))

    return df


def write_companies_df_to_csv(df: DataFrame, path_prefix: Path) -> None:
    """Write the companies DataFrame to CSV files to be emitted as FtM in the ru_egrul crawler."""
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
    directorships_df = directorships_df.withColumn(
        "director_id", col("director.id")
    ).drop("director")

    successions_df = df.withColumn("succession", explode(col("successions"))).select(
        "succession.*"
    )

    successor_companies_df = successions_df.where(col("successor").isNotNull()).select(
        "successor.*"
    )
    predecessor_companies_df = successions_df.where(
        col("predecessor").isNotNull()
    ).select("predecessor.*")

    successions_df = successions_df.drop("successor").drop("predecessor")

    # Owner can be one of three schemas, split the union type up
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

    # This is what's required for Python to write a CSV with no further options
    csv_options = {"header": True, "escape": '"', "mode": "overwrite"}
    ownerships_df.write.csv(str(path_prefix / "ownerships"), **csv_options)
    directorships_df.write.csv(str(path_prefix / "directorships"), **csv_options)
    successions_df.write.csv(str(path_prefix / "successions"), **csv_options)
    persons_df.write.csv(str(path_prefix / "persons"), **csv_options)
    all_legal_entities_df.write.csv(str(path_prefix / "legalentities"), **csv_options)


def get_archive_date(archive_path: Path) -> date:
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


def crawl(context: Context) -> None:
    spark = SparkSession.builder.appName("ru_egrul").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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
        if date(2022, 1, 1) <= d <= date(2022, 12, 31)
    ]

    current_companies = None
    for archive_date, archives in archives_by_date:
        context.log.info("Processing %s" % archive_date)
        crawled = crawl_archives_for_date(spark, archive_date, archives)
        # First iteration, set current_companies to the first crawled DataFrame
        if current_companies is None:
            current_companies = crawled
            continue

        current_companies = update_companies_from_new_companies(
            context, current_companies, crawled
        )
        context.log.info(
            "Updated state has %d company records" % current_companies.count()
        )

    write_companies_df_to_csv(
        current_companies,
        Path(LOCAL_BUCKET_PATH, INTERNAL_DATA_PROCESSESED_PREFIX, "current"),
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
