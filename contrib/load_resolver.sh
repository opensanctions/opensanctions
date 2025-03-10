set -euo pipefail

JSON_PATH=data/resolve.ijson
SQLITE_PATH=data/resolver.sqlite3

echo "Deleting old resolver data ($JSON_PATH, $SQLITE_PATH)"
rm -f $JSON_PATH $SQLITE_PATH
gcloud storage cp gs://resolver-backups.opensanctions.org/resolve.ijson $JSON_PATH

echo "Loading resolver data from $JSON_PATH"
export NOMENKLATURA_DB_URL=sqlite:///$SQLITE_PATH
nomenklatura load-resolver $JSON_PATH

echo "Resolver loaded to $NOMENKLATURA_DB_URL"
