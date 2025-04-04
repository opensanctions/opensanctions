from pyspark.sql.types import StringType, StructType, StructField, DateType, ArrayType

entity_fields = [
    StructField("id", StringType(), False),
    StructField("seen_date", DateType(), False),
]

person_schema = StructType(
    entity_fields
    + [
        # These names are raw, no splittin or abbreviation replacement
        StructField("name", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("father_name", StringType(), True),
        StructField("inn_code", StringType(), True),
        StructField("countries", ArrayType(StringType()), True),
    ]
)

legal_entity_schema = StructType(
    entity_fields
    + [
        # These names are raw, no splitting or abbreviation replacement
        # For LegalEntity and Organization
        StructField("name", StringType(), True),
        StructField("name_latin", StringType(), True),
        # For Company
        StructField("name_full", StringType(), True),
        StructField("name_short", StringType(), True),
        StructField("legal_form", StringType(), True),
        StructField("country", StringType(), True),
        StructField("jurisdiction", StringType(), True),
        StructField("inn_code", StringType(), True),
        StructField("ogrn_code", StringType(), True),
        StructField("kpp_code", StringType(), True),
        StructField("incorporation_date", DateType(), True),
        StructField("dissolution_date", DateType(), True),
        StructField("email", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("addresses", ArrayType(StringType()), True),
        StructField("schema", StringType(), False),
    ]
)

succession_schema = StructType(
    entity_fields
    + [
        # Only one must be set, the other is the parent and will be null
        StructField("successor", legal_entity_schema, True),
        StructField("predecessor", legal_entity_schema, True),
        # For ease of emission, both of them are set
        StructField("successor_id", StringType(), True),
        StructField("predecessor_id", StringType(), True),
    ]
)

owner_schema = StructType(
    [
        StructField("person", person_schema, True),
        StructField("legal_entity", legal_entity_schema, True),
    ]
)

ownership_schema = StructType(
    entity_fields
    + [
        # Only there for easy emission, always the parent
        StructField("asset_id", StringType(), False),
        StructField("owner", owner_schema, False),
        StructField("summary_1", StringType(), True),
        StructField("summary_2", StringType(), True),
        StructField("record_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("start_date", DateType(), True),
        StructField("shares_count", StringType(), True),
        StructField("percentage", StringType(), True),
    ]
)

directorship_schema = StructType(
    entity_fields
    + [
        # Only there for easy emission, always the parent
        StructField("organization_id", StringType(), True),
        StructField("director", person_schema, True),
        StructField("role", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
    ]
)

# Define schema for the main structure
company_record_schema = StructType(
    [
        # Only there for easy joining, could be omitted in the future
        StructField("id", StringType(), False),
        StructField("legal_entity", legal_entity_schema, False),
        StructField("successions", ArrayType(succession_schema), True),
        StructField("ownerships", ArrayType(ownership_schema), True),
        StructField("directorships", ArrayType(directorship_schema), True),
    ]
)
