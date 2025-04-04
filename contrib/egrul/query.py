from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, udf, explode, length, size
from pyspark.sql.types import BooleanType

from schema import company_record_schema

spark = (
    SparkSession.builder.appName("ru_egrul")
    .master("local[4]")
    .config("spark.driver.memory", "8g")
    .enableHiveSupport()
    .getOrCreate()
)

# Use bpython -i contrib/egrul/query.py to get an interactive query console
# where you can use spark.sql() to run SQL queries on the tables.
