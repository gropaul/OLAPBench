import argparse
import decimal
import json
import os
import pathlib
import struct
from typing import Literal
import duckdb
import uuid
from duckdb.sqltypes import VARCHAR, BIGINT

from benchmarks import benchmark
from benchmarks.tpch.utils import TPC_ID_TYPES, TABLE_ID_COLUMNS, convert_id
from util import sql




def create_schemas():
    path = 'benchmarks/tpch/tpch.dbschema.json'
    for id_type in TPC_ID_TYPES:
        schema = json.load(open(path, 'r'))
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
            columns_to_convert = TABLE_ID_COLUMNS.get(table_name, [])
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


class TPCH(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None,
                 excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.scale = args["scale"]
        self.zipf = args["zipf"] if "zipf" in args.keys() else 0
        self.id_type = args["id_type"] if "id_type" in args.keys() else "base64_32_bytes"

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "tpch" + f"_{self.id_type}"

    @property
    def description(self) -> str:
        return "TPC-H Benchmark"

    @property
    def unique_name(self) -> str:
        return f"tpchSf{self.scale}" + ("" if self.zipf == 0 else f"Skew{self.zipf}") + f"IdType_{self.id_type}"

    @property
    def data_dir(self) -> str:
        return os.path.join("tpch", f"sf{self.scale}" if self.zipf == 0 else f"sf{self.scale}skew{self.zipf}")

    def dbgen(self):
        script_name = f'dbgen{"" if self.zipf == 0 else "Skewed"}.sh'
        script_path = os.path.join(self.path, script_name)
        command = f'{script_path} {self.scale}{"" if self.zipf == 0 else " " + str(self.zipf)}'
        self._load_with_command(command)
        self._post_process()

    def _post_process(self):

        create_schemas()
        convert_id_bound = lambda original_id: convert_id(original_id, self.id_type)
        return_type = BIGINT if self.id_type in ['int64_sorted', 'int64_random'] else VARCHAR

        # remove existing tmp.duckdb if exists
        if os.path.exists('tmp.duckdb'):
            os.remove('tmp.duckdb')
        con = duckdb.connect('tmp.duckdb')
        con.create_function('convert_id', convert_id_bound, [BIGINT], return_type)

        schema = self.get_schema(path='benchmarks/tpch/tpch.dbschema.json')
        statements = sql.create_table_statements(schema, alter_table=False)
        for stmt in statements:
            con.execute(stmt)

        for table, columns in TABLE_ID_COLUMNS.items():
            table_path = os.path.join("data", self.data_dir, f"{table}.tbl")
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

            table_parquet_path = os.path.join("data", self.data_dir, f"{table}.transformed.tbl")

            # remove existing transformed file if exists
            if os.path.exists(table_parquet_path):
                os.remove(table_parquet_path)

            query = f"""
                COPY (SELECT {', '.join(column_names_transformed)} FROM {table})
                TO '{table_parquet_path}'
                (FORMAT CSV, DELIMITER '|', HEADER FALSE);
            """
            con.execute(query)

    def empty(self) -> bool:
        return self.scale == 0


class TPCHDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "tpch"

    @staticmethod
    def get_description() -> str:
        return "TPC-H Benchmark"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-s", "--scale", dest="scale", type=decimal.Decimal, default=1,
                            help="scale factor (default: 1)")
        parser.add_argument("-z", "--zipf", dest="zipf", type=decimal.Decimal, default=0,
                            help="zipfian skew (default: 0)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None,
                    excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return TPCH(base_dir, args, included_queries, excluded_queries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    TPCHDescription.add_arguments(parser)
    args = parser.parse_args()

    benchmark_instance = TPCHDescription.instantiate("", vars(args))
    benchmark_instance.dbgen()
