import base64
import hashlib
import struct
import uuid
import time
from typing import Literal, List

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
TABLE_ID_COLUMNS: dict[str, list[str]] = {
    "part": ["p_partkey"],
    "region": ["r_regionkey"],
    "nation": ["n_nationkey", "n_regionkey"],
    "supplier": ["s_suppkey", "s_nationkey"],
    "partsupp": ["ps_partkey", "ps_suppkey"],
    "customer": ["c_custkey", "c_nationkey"],
    "orders": ["o_orderkey", "o_custkey"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey"],
}


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
