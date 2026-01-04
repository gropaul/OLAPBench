import base64
import hashlib
import json
import os
import struct
import uuid
from typing import Literal, List, Dict
import duckdb
from duckdb.sqltypes import VARCHAR, BIGINT

from util import sql

# more types of ids are here https://www.kaggle.com/datasets/seanlahman/the-history-of-baseball?select=all_star.csv
TPC_ID_TYPE = Literal["int64_sorted", "int64_random", "uuid_v4", "uuid_v7", "base64_16_bytes", "base64_32_bytes"]
TPC_ID_TYPES: List[TPC_ID_TYPE] = [
    "int64_sorted",
    "int64_random",
    "uuid_v4",
    "uuid_v7",
    "base64_16_bytes",
    "base64_32_bytes",
]

# for every table a list of columns that need to be converted to the specified id type
TPC_H_TABLE_ID_COLUMNS: dict[str, list[str]] = {
    "part": ["p_partkey"],
    "region": ["r_regionkey"],
    "nation": ["n_nationkey", "n_regionkey"],
    "supplier": ["s_suppkey", "s_nationkey"],
    "partsupp": ["ps_partkey", "ps_suppkey"],
    "customer": ["c_custkey", "c_nationkey"],
    "orders": ["o_orderkey", "o_custkey"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey"],
}

TPC_DS_TABLE_ID_COLUMNS: dict[str, list[str]] = {
    'customer_address': ['ca_address_sk'],
    'customer_demographics': ['cd_demo_sk'],
    'date_dim': ['d_date_sk'],
    'warehouse': ['w_warehouse_sk'],
    'ship_mode': ['sm_ship_mode_sk'],
    'time_dim': ['t_time_sk'],
    'reason': ['r_reason_sk'],
    'income_band': ['ib_income_band_sk'],
    'household_demographics': ['hd_demo_sk'],
    'item': ['i_item_sk'],
    'store': ['s_store_sk'],
    'call_center': ['cc_call_center_sk'],
    'customer': ['c_customer_sk'],
    'web_site': ['web_site_sk'],
    'web_page': ['wp_web_page_sk'],
    'promotion': ['p_promo_sk'],
    'catalog_page': ['cp_catalog_page_sk'],
    'inventory': ['inv_date_sk', 'inv_item_sk', 'inv_warehouse_sk'],
    'web_sales': ['ws_item_sk', 'ws_order_number'],
    'catalog_sales': ['cs_item_sk', 'cs_order_number'],
    'store_sales': ['ss_item_sk', 'ss_ticket_number'],
    'web_returns': ['wr_item_sk', 'wr_order_number'],
    'catalog_returns': ['cr_item_sk', 'cr_order_number'],
    'store_returns': ['sr_item_sk', 'sr_ticket_number'],
}


def create_string_id_data(benchmark, base_schema_path: str, table_columns_map: Dict):
    create_new_schemas(base_schema_path, table_columns_map)
    convert_id_bound = lambda original_id: convert_id(original_id, benchmark.id_type)
    return_type = BIGINT if benchmark.id_type in ['int64_sorted', 'int64_random'] else VARCHAR

    # remove existing tmp.duckdb if exists
    if os.path.exists('tmp.duckdb'):
        os.remove('tmp.duckdb')
    con = duckdb.connect('tmp.duckdb')
    con.create_function('convert_id', convert_id_bound, [BIGINT], return_type)

    schema = benchmark.get_schema(path=base_schema_path)
    statements = sql.create_table_statements(schema, alter_table=False)
    for stmt in statements:
        con.execute(stmt)

    for table, columns in TPC_H_TABLE_ID_COLUMNS.items():
        table_path = os.path.join("data", benchmark.data_dir, f"{table}.tbl")
        # first copy to .tbl to a temporary table with all original columns
        copy_to_temp = f"""
            INSERT INTO {table}
            SELECT *
            FROM read_csv('{table_path}');
        """
        con.execute(copy_to_temp)
        # get the column names from table schema
        column_names_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';"
        result = con.execute(column_names_query).fetchall()
        column_names = [row[0] for row in result]
        column_names_transformed = []
        for col in column_names:
            if col in columns:
                column_names_transformed.append(f"convert_id({col}::BIGINT) AS {col}")
            else:
                column_names_transformed.append(col)

        table_parquet_path = os.path.join("data", benchmark.data_dir, f"{table}.transformed.tbl")

        # remove existing transformed file if exists
        if os.path.exists(table_parquet_path):
            os.remove(table_parquet_path)

        query = f"""
            COPY (SELECT {', '.join(column_names_transformed)} FROM {table})
            TO '{table_parquet_path}'
            (FORMAT CSV, DELIMITER '|', HEADER FALSE);
        """
        con.execute(query)


def create_new_schemas(base_schema_path: str, table_columns_map: Dict):
    for id_type in TPC_ID_TYPES:
        schema = json.load(open(base_schema_path, 'r'))
        schema['file_ending'] = 'transformed.tbl'
        schema['quote'] = '"'
        schema['format'] = 'csv'
        # schema['file_ending'] = 'parquet'
        # schema['format'] = 'parquet'
        # del schema['delimiter']
        # del schema['null']
        for table in schema['tables']:
            table_name = table['name']
            columns = table['columns']
            columns_to_convert = table_columns_map.get(table_name, [])
            for column in columns:
                if column['name'] in columns_to_convert:
                    if id_type in ['int64_sorted', 'int64_random']:
                        column['type'] = 'BIGINT'
                    else:
                        column['type'] = 'VARCHAR'
            table['columns'] = columns
        new_path = f'benchmarks/tpch/tpch_{id_type}.dbschema.json'
        with open(new_path, 'w') as f:
            json.dump(schema, f, indent=2)



def _hash_bytes(original_id: int, n_bytes: int) -> bytes:
    return hashlib.sha256(str(original_id).encode()).digest()[:n_bytes]


def _uuid_v7_from_int(original_id: int) -> str:
    # --- 1. Deterministic timestamp (ms) ---
    # Anchor around a fixed epoch to keep ordering stable
    BASE_TS_MS = 1700000000000  # arbitrary fixed base (2023-11)
    ts_ms = BASE_TS_MS + (original_id & ((1 << 48) - 1))

    # --- 2. Hash for randomness ---
    h = hashlib.blake2b(
        str(original_id).encode(),
        digest_size=16
    ).digest()

    rand_a = int.from_bytes(h[0:2], "big") & 0x0FFF      # 12 bits
    rand_b = int.from_bytes(h[2:10], "big") & ((1 << 62) - 1)

    # --- 3. Assemble UUID fields ---
    time_low = (ts_ms >> 16) & 0xFFFFFFFF
    time_mid = ts_ms & 0xFFFF

    time_hi_and_version = (0x7 << 12) | rand_a  # version 7

    clock_seq = (0b10 << 14) | ((rand_b >> 48) & 0x3FFF)  # variant
    node = rand_b & ((1 << 48) - 1)

    return str(uuid.UUID(fields=(
        time_low,
        time_mid,
        time_hi_and_version,
        (clock_seq >> 8) & 0xFF,
        clock_seq & 0xFF,
        node
    )))



def convert_id(original_id: int, id_type: TPC_ID_TYPE):
    # default / sorted
    if id_type == "int64_sorted":
        return original_id

    # deterministic, positive, fits into signed int64
    if id_type == "int64_random":
        h = hashlib.blake2b(
            str(original_id).encode(),
            digest_size=8
        ).digest()

        u = struct.unpack(">Q", h)[0]  # uint64
        return u & ((1 << 63) - 1)  # keep only lower 63 bits

    # deterministic uuidv4 (hash-based)
    if id_type == "uuid_v4":
        h = hashlib.md5(str(original_id).encode()).digest()
        return str(uuid.UUID(bytes=h))

    if id_type == "uuid_v7":
        return _uuid_v7_from_int(original_id)

    # base64, fixed length
    if id_type == "base64_16_bytes":
        raw = _hash_bytes(original_id, 16)
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    if id_type == "base64_32_bytes":
        raw = _hash_bytes(original_id, 32)
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    raise ValueError(f"Unknown TPC_ID_TYPE: {id_type}")
