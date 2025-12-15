#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating TPC-H UUID database with scale factor $SF"

# Directory for UUID data
UUID_DIR="data/tpch_uuid/sf$SF"
TPCH_DIR="data/tpch/sf$SF"

mkdir -p "$UUID_DIR"

# First generate standard TPC-H data if not exists
if [ ! -d "$TPCH_DIR" ] || [ -z "$(ls -A "$TPCH_DIR" 2>/dev/null)" ]; then
  echo "Generating base TPC-H data first..."
  mkdir -p "$TPCH_DIR"
  cd "data/tpch/"
  (
    curl -OL --no-progress-meter https://db.in.tum.de/~fent/dbgen/tpch/tpch-kit.zip
    unzip -q -u tpch-kit.zip

    cd tpch-kit-852ad0a5ee31ebefeed884cea4188781dd9613a3/dbgen
    rm -rf ./*.tbl
    MACHINE=LINUX make -sj "$(nproc)" dbgen 2>/dev/null
    ./dbgen -f -s "$SF"
    for table in ./*.tbl; do
      sed 's/|$//' "$table" >"../../sf$SF/$table"
      rm "$table"
    done
  )
  cd ../..
fi

# Convert integer keys to UUID strings if not already done
if [ -z "$(ls -A "$UUID_DIR" 2>/dev/null)" ]; then
  echo "Converting integer keys to UUID strings..."

  # Python script to convert keys to UUIDs
  export SF
  python3 << 'PYTHON_SCRIPT'
import os
import hashlib

SF = os.environ['SF']
TPCH_DIR = f"data/tpch/sf{SF}"
UUID_DIR = f"data/tpch_uuid/sf{SF}"

def int_to_uuid(val: int, prefix: str = "") -> str:
    """Convert an integer to a deterministic 32-character UUID-like string."""
    # Create a deterministic hash from the integer and prefix
    data = f"{prefix}:{val}".encode('utf-8')
    hash_bytes = hashlib.md5(data).hexdigest()
    return hash_bytes  # Already 32 characters

# Define which columns need to be converted for each table
# Format: (table_name, [(column_index, key_prefix), ...])
table_configs = [
    ("part", [(0, "part")]),  # p_partkey
    ("region", [(0, "region")]),  # r_regionkey
    ("nation", [(0, "nation"), (2, "region")]),  # n_nationkey, n_regionkey
    ("supplier", [(0, "supplier"), (3, "nation")]),  # s_suppkey, s_nationkey
    ("customer", [(0, "customer"), (3, "nation")]),  # c_custkey, c_nationkey
    ("partsupp", [(0, "part"), (1, "supplier")]),  # ps_partkey, ps_suppkey
    ("orders", [(0, "order"), (1, "customer")]),  # o_orderkey, o_custkey
    ("lineitem", [(0, "order"), (1, "part"), (2, "supplier")]),  # l_orderkey, l_partkey, l_suppkey
]

for table_name, columns_to_convert in table_configs:
    input_file = os.path.join(TPCH_DIR, f"{table_name}.tbl")
    output_file = os.path.join(UUID_DIR, f"{table_name}.tbl")

    print(f"Processing {table_name}...")

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            fields = line.rstrip('\n').split('|')

            for col_idx, prefix in columns_to_convert:
                if col_idx < len(fields) and fields[col_idx]:
                    fields[col_idx] = int_to_uuid(int(fields[col_idx]), prefix)

            outfile.write('|'.join(fields) + '\n')

print("Done!")
PYTHON_SCRIPT

fi
