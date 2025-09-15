from pyspark.sql.types import StringType, StructType, StructField, DateType, ArrayType

entity_fields = [
    StructField("id", StringType(), nullable=False),
    StructField("seen_date", DateType(), nullable=False),
]

person_schema = StructType(
    entity_fields
    + [
        # These names are raw, no splittin or abbreviation replacement
        StructField("name", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("father_name", StringType()),
        StructField("inn_code", StringType()),
        StructField("countries", ArrayType(StringType())),
    ]
)

legal_entity_schema = StructType(
    entity_fields
    + [
        # These names are raw, no splitting or abbreviation replacement
        # For LegalEntity and Organization
        StructField("name", StringType()),
        StructField("name_latin", StringType()),
        # For Company
        StructField("name_full", StringType()),
        StructField("name_short", StringType()),
        StructField("legal_form", StringType()),
        StructField("country", StringType()),
        StructField("jurisdiction", StringType()),
        StructField("inn_code", StringType()),
        StructField("ogrn_code", StringType()),
        StructField("kpp_code", StringType()),
        StructField("incorporation_date", DateType()),
        StructField("dissolution_date", DateType()),
        StructField("email", StringType()),
        StructField("publisher", StringType()),
        StructField("registration_number", StringType()),
        StructField("addresses", ArrayType(StringType())),
        StructField("schema", StringType(), nullable=False),
    ]
)

succession_schema = StructType(
    entity_fields
    + [
        # Only one must be set, the other is the parent and will be null
        StructField("successor", legal_entity_schema),
        StructField("predecessor", legal_entity_schema),
        # For ease of emission, both of them are set
        StructField("successor_id", StringType()),
        StructField("predecessor_id", StringType()),
    ]
)

owner_schema = StructType(
    [
        StructField("person", person_schema),
        StructField("legal_entity", legal_entity_schema),
    ]
)

ownership_schema = StructType(
    entity_fields
    + [
        # Only there for easy emission, always the parent
        StructField("asset_id", StringType(), nullable=False),
        StructField("owner", owner_schema, nullable=False),
        StructField("summary_1", StringType()),
        StructField("summary_2", StringType()),
        StructField("record_id", StringType()),
        StructField("date", DateType()),
        StructField("end_date", DateType()),
        StructField("start_date", DateType()),
        StructField("shares_count", StringType()),
        StructField("percentage", StringType()),
    ]
)

directorship_schema = StructType(
    entity_fields
    + [
        # Only there for easy emission, always the parent
        StructField("organization_id", StringType()),
        StructField("director", person_schema),
        StructField("role", StringType()),
        StructField("summary", StringType()),
        StructField("start_date", DateType()),
        StructField("end_date", DateType()),
    ]
)

# Define schema for the main structure
company_record_schema = StructType(
    [
        # Only there for easy joining, could be omitted in the future
        StructField("id", StringType(), nullable=False),
        StructField("legal_entity", legal_entity_schema, nullable=False),
        StructField("successions", ArrayType(succession_schema)),
        StructField("ownerships", ArrayType(ownership_schema)),
        StructField("directorships", ArrayType(directorship_schema)),
    ]
)
