from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, udf, explode, length, size
from pyspark.sql.types import BooleanType

from schema import company_record_schema

spark = (
    SparkSession.builder.appName("ru_egrul")
    .master("local[4]")
    .getOrCreate()
)

def custom_join(old, new):
    # old_ids = map(lambda o: o["id"], old["ownerships"])
    # new_ids = map(lambda o: o["id"], new["ownerships"])
    #
    # if set(old_ids) != set(new_ids):
    #     print("Old:", old["ownerships"])
    #     print("New:", new["ownerships"])
    if old is None and new is not None:
        return new
    if new is None and old is not None:
        return old


    return new

def check_ownerships_differs(old, new) -> bool:
    old_ids = map(lambda o: o["id"], old["ownerships"])
    new_ids = map(lambda o: o["id"], new["ownerships"])

    return set(old_ids) != set(new_ids)


# old = spark.read.parquet("/home/leon/internal-data/ru_egrul/cache/2022-01-01.parquet")
# new = spark.read.parquet("/home/leon/internal-data/ru_egrul/cache/2022-01-24.parquet")
#
# # #old = old.limit(100000)
# # old_rdd = old.rdd.map(lambda x: (x["id"], x))
# # new_rdd = new.rdd.map(lambda x: (x["id"], x))
# #
# # merged_rdd = old_rdd.join(new_rdd).filter(check_ownerships_differs).map(custom_join)
# #
# # merged = merged_rdd.toDF(company_schema)
#
# old_struct = old.select(struct(*old.columns).alias("old"))
# new_struct = new.select(struct(*new.columns).alias("new"))
#
# old_with_key = old_struct.withColumn("join_key", col("old.id"))
# new_with_key = new_struct.withColumn("join_key", col("new.id"))
#
# joined = old_with_key.join(new_with_key, "join_key", "inner")
#
# joined.write.parquet("/home/leon/internal-data/ru_egrul/cache/inner.parquet")

#merged = joined.withColumn("record", udf(custom_join, company_schema)(col("old"), col("new")))

#fold = spark.read.parquet("/Users/leon/internal-data/ru_egrul/cache/folded.parquet")
#fold = fold.withColumn("ownership", explode("ownerships"))
#fold_expl = fold.withColumn("ownership", explode("ownerships"))

full = spark.read.parquet("/Users/leon/internal-data/ru_egrul/cache/2022-01-24.parquet")
x = full.select(size(col("addresses")).alias("num_addresses"))
#full_expl = fold.withColumn("ownership", explode("ownerships"))
# new_ownerships = joined.filter(udf(check_ownerships_differs, BooleanType())(col("old"), col("new"))) \
#     .withColumn("merged", udf(custom_join, company_schema)(col("old"), col("new"))) \
#     .select("merged.*")