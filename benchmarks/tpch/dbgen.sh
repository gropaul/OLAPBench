#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating TPC-H database with scale factor $SF"

mkdir -p "data/tpch/sf$SF"
cd "data/tpch/"

# Reuse existing datasets
if [ -z "$(ls -A "sf$SF")" ]; then
  (
    curl -OL --no-progress-meter https://db.in.tum.de/~fent/dbgen/tpch/tpch-kit.zip
#    echo '3045d758d1506ce37a354f1b34d44129  tpch-kit.zip' | md5sum --check --status
    unzip -q -u tpch-kit.zip

    cd tpch-kit-852ad0a5ee31ebefeed884cea4188781dd9613a3/dbgen
    rm -rf ./*.tbl
    MACHINE=LINUX make -sj 4 dbgen 2>/dev/null
    ./dbgen -f -s "$SF"
    for table in ./*.tbl; do
      sed 's/|$//' "$table" >"../../sf$SF/$table"
      rm "$table"
    done
  )
fi
