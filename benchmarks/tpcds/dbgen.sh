#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating TPC-DS database with scale factor $SF"

mkdir -p "data/tpcds/sf$SF"
cd "data/tpcds/"

# Reuse existing datasets
if [ -z "$(ls -A "sf$SF")" ]; then
  (
    duckdb -c "CALL dsdgen(sf = $SF); EXPORT DATABASE 'sf$SF' (FORMAT csv, DELIMITER '|', HEADER False);"
    # rename all .csv to .tbl
    for file in sf$SF/*.csv; do
      mv "$file" "${file%.csv}.tbl"
    done
  )
fi
