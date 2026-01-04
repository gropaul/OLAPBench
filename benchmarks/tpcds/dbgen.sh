#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating TPC-DS database with scale factor $SF"

mkdir -p "data/tpcds/sf$SF"
cd "data/tpcds/"

# Reuse existing datasets
if [ -z "$(ls -A "sf$SF")" ]; then
  (
    if [ ! -d tpcds-kit ]; then
      git clone --depth=1 ssh://git@gitlab.db.in.tum.de:2222/fent/tpcds-kit.git
    else
      (
        cd tpcds-kit
      )
    fi

    cd tpcds-kit/tools
    rm -rf ./*.dat

    make clean
    make dsdgen

    ./dsdgen -FORCE -SCALE "$SF"
    for table in ./*.dat; do
      sed 's/|$//' "$table" >"../../sf$SF/$table"
      rm "$table"
    done
  )
fi
