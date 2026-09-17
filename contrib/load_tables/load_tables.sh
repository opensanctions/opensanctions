#!/usr/bin/env bash
# Replace the contents of local zavod tables with production dumps.
#
# The dumps are gzipped CSVs written daily by the etl-prod-dev-dumps job
# to the prod-etl-dev-dumps bucket.
# Their header row names the columns in table definition order.
#
#   contrib/load_tables/load_tables.sh TABLE=SOURCE [TABLE=SOURCE ...]
#
# SOURCE is a local .csv.gz or a gs:// URI (fetched with gcloud storage cp).
#
# The target database is read from ZAVOD_DATABASE_URI. Supported schemes:
#   postgresql://  loaded with psql \copy
#   sqlite:///     loaded with sqlite3 .import
# The script refuses to run when the connection string mentions "prod", and
# asks for confirmation before truncating anything.
set -euo pipefail

usage() {
	echo "Usage: $0 TABLE=SOURCE [TABLE=SOURCE ...]" >&2
	echo "  SOURCE is a local .csv.gz file or a gs:// URI" >&2
	exit 2
}

[[ $# -ge 1 ]] || usage
TABLES=()
SOURCES=()
for arg in "$@"; do
	[[ "$arg" == *=* ]] || usage
	table="${arg%%=*}"
	source="${arg#*=}"
	[[ "$table" =~ ^[a-z_]+$ ]] || {
		echo "Bad table name: $table" >&2
		exit 1
	}
	TABLES+=("$table")
	SOURCES+=("$source")
done

DB_URL="${ZAVOD_DATABASE_URI:?Set ZAVOD_DATABASE_URI to the local database to load into}"

# Never load over production. Local databases have no business being called
# anything like it either.
if printf '%s' "$DB_URL" | grep -qi prod; then
	echo "Refusing to load: ZAVOD_DATABASE_URI mentions prod: $DB_URL" >&2
	exit 1
fi

case "$DB_URL" in
postgresql://*) DIALECT=postgres ;;
sqlite:///*) DIALECT=sqlite ;;
*)
	echo "Unsupported ZAVOD_DATABASE_URI scheme: $DB_URL" >&2
	exit 1
	;;
esac

table_exists() {
	local table="$1"
	case "$DIALECT" in
	postgres) psql -qtA "$DB_URL" -c "SELECT 1 FROM information_schema.tables WHERE table_name = '$table'" ;;
	sqlite) sqlite3 "${DB_URL#sqlite:///}" "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '$table'" ;;
	esac
}

# The tables are created by zavod on first use, e.g. by running a crawler.
for table in "${TABLES[@]}"; do
	[[ -n "$(table_exists "$table")" ]] || {
		echo "Table $table does not exist in the database. Run zavod against it once to create the tables." >&2
		exit 1
	}
done

MASKED_URL=$(printf '%s' "$DB_URL" | sed -E 's#(://[^:/@]+):[^@]*@#\1:***@#')
echo "About to replace the contents of these tables in $MASKED_URL:"
for i in "${!TABLES[@]}"; do
	echo "  ${TABLES[$i]}  <-  ${SOURCES[$i]}"
done
read -r -p "Type yes to continue: " answer
[[ "$answer" == "yes" ]] || {
	echo "Aborted." >&2
	exit 1
}

DUMP_DIR=data/dev-dumps
mkdir -p "$DUMP_DIR"
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/load_tables.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT

load_postgres() {
	local table="$1" csv="$2" header="$3"
	# Quote every column name: "user" in the resolver table is a keyword.
	local columns
	columns=$(printf '%s' "$header" | sed -E 's/[^,]+/"&"/g')
	{
		echo "BEGIN;"
		echo "TRUNCATE \"$table\";"
		echo "\\copy \"$table\" ($columns) FROM '$csv' WITH (FORMAT csv, HEADER)"
		if [[ ",$header," == *,id,* ]]; then
			# The dump keeps the ids, so move the sequence past them.
			echo "\\o /dev/null"
			echo "SELECT setval(pg_get_serial_sequence('$table', 'id'), (SELECT MAX(id) FROM \"$table\"));"
			echo "\\o"
		fi
		echo "COMMIT;"
	} | psql -v ON_ERROR_STOP=1 -q "$DB_URL"
}

load_sqlite() {
	local table="$1" csv="$2"
	local db_path="${DB_URL#sqlite:///}"
	local staging="${table}_import"
	# .import makes every staging column TEXT, and reads empty fields as ''.
	# Build the INSERT from the real table's definition: NULL for empty
	# nullable columns, 0/1 for the t/f Postgres writes for booleans.
	local insert
	insert=$(sqlite3 -bail "$db_path" <<SQL
SELECT 'INSERT INTO "$table" (' || group_concat('"' || name || '"', ', ') || ')'
    || ' SELECT ' || group_concat(expr, ', ') || ' FROM "$staging";'
FROM (
    SELECT name,
        CASE
            WHEN type = 'BOOLEAN' THEN 'CASE "' || name || '" WHEN ''t'' THEN 1 WHEN ''f'' THEN 0 END'
            WHEN "notnull" = 0 THEN 'NULLIF("' || name || '", '''')'
            ELSE '"' || name || '"'
        END AS expr
    FROM pragma_table_info('$table')
    ORDER BY cid
);
SQL
	)
	[[ -n "$insert" ]] || {
		echo "No such table in $db_path: $table" >&2
		exit 1
	}
	sqlite3 -bail "$db_path" <<SQL
DROP TABLE IF EXISTS "$staging";
.import --csv '$csv' $staging
BEGIN;
DELETE FROM "$table";
$insert
DROP TABLE "$staging";
COMMIT;
SQL
}

count_rows() {
	local table="$1"
	case "$DIALECT" in
	postgres) psql -qtA "$DB_URL" -c "SELECT COUNT(*) FROM \"$table\"" ;;
	sqlite) sqlite3 "${DB_URL#sqlite:///}" "SELECT COUNT(*) FROM \"$table\"" ;;
	esac
}

for i in "${!TABLES[@]}"; do
	table="${TABLES[$i]}"
	source="${SOURCES[$i]}"
	if [[ "$source" == gs://* ]]; then
		dump="$DUMP_DIR/$(basename "$source")"
		echo "Downloading $source to $dump..."
		gcloud storage cp "$source" "$dump"
	else
		dump="$source"
	fi
	[[ -f "$dump" ]] || {
		echo "No such file: $dump" >&2
		exit 1
	}
	csv="$WORK_DIR/$table.csv"
	gzip -dc "$dump" >"$csv"
	header=$(head -n 1 "$csv" | tr -d '\r')
	echo "Loading $table..."
	case "$DIALECT" in
	postgres) load_postgres "$table" "$csv" "$header" ;;
	sqlite) load_sqlite "$table" "$csv" ;;
	esac
	rm -f "$csv"
	echo "Loaded $(count_rows "$table") rows into $table."
done
echo "Done."
