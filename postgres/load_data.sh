#!/usr/bin/env bash
set -euo pipefail

PSQL="psql -v ON_ERROR_STOP=1 --username ${POSTGRES_USER} --dbname ${POSTGRES_DB}"

echo ">>> loading CSV files from /csv into raw_data ..."

shopt -s nullglob
count=0
for f in /csv/MOCK_DATA*.csv; do
    echo "    \\copy raw_data FROM '$f'"
    $PSQL -c "\copy raw_data FROM '$f' WITH (FORMAT csv, HEADER true)"
    count=$((count + 1))
done

ROWS=$($PSQL -tAc "SELECT COUNT(*) FROM raw_data;")
echo ">>> raw_data loaded: ${ROWS} rows from ${count} files"
