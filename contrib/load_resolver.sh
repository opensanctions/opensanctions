set -euo pipefail

JSON_PATH=data/resolve.ijson

rm -f $JSON_PATH 
gcloud storage cp gs://resolver-backups.opensanctions.org/resolve.ijson $JSON_PATH

echo "Loading resolver data from $JSON_PATH to $NOMENKLATURA_DB_URL"
nomenklatura load-resolver "$JSON_PATH"
echo "Resolver loaded to $NOMENKLATURA_DB_URL"
