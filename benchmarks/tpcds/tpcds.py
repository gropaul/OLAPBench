import argparse
import os
import pathlib

from benchmarks import benchmark
from benchmarks.tpch.utils import create_string_id_data, TPC_DS_TABLE_ID_COLUMNS, TPC_ID_TYPE, convert_id


class TPCDS(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.scale = args["scale"]
        self.id_type: TPC_ID_TYPE = args["id_type"] if "id_type" in args.keys() else "uuid_v4"

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "tpcds" + f"_{self.id_type}"

    @property
    def description(self) -> str:
        return "TPC-DS Benchmark"

    @property
    def unique_name(self) -> str:
        return f"tpcdsSf{self.scale}" + f"IdType_{self.id_type}"

    @property
    def data_dir(self) -> str:
        return os.path.join("tpcds", f"sf{self.scale}")

    def dbgen(self):
        self._load_with_command(f'{os.path.join(self.path, "dbgen.sh")} {self.scale}')
        self._post_process()

    def _post_process(self):
        create_string_id_data(self, 'benchmarks/tpcds/tpcds.dbschema.json', TPC_DS_TABLE_ID_COLUMNS)

    def post_process_queries(self, queries: list[tuple[str, str]]) -> list[tuple[str, str]]:

        def convert_id_for_sql(value: int) -> str:
            convert_id_bound = lambda original_id: convert_id(original_id, self.id_type)
            query_9_converted_id = convert_id_bound(1)
            if isinstance(query_9_converted_id, str):
                return f"'{query_9_converted_id}'"
            return str(query_9_converted_id)

        # find query 9.sql
        query_9_index = queries.index(next(filter(lambda x: x[0] == '9.sql', queries), None))
        query_9_name, query_9_sql = queries[query_9_index]
        query_9_sql = query_9_sql.replace('r_reason_sk = 1', f'r_reason_sk = {convert_id_for_sql(1)}')
        queries[query_9_index] = (query_9_name, query_9_sql)

        # find query 44.sql: where ss_store_sk = 29
        query_44_index = queries.index(next(filter(lambda x: x[0] == '44.sql', queries), None))
        query_44_name, query_44_sql = queries[query_44_index]
        query_44_sql = query_44_sql.replace('ss_store_sk = 29',
                                                f'ss_store_sk = {convert_id_for_sql(29)}')
        queries[query_44_index] = (query_44_name, query_44_sql)

        # find query 45.sql: where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
        query_45_index = queries.index(next(filter(lambda x: x[0] == '45.sql', queries), None))
        query_45_name, query_45_sql = queries[query_45_index]
        query_45_sql = query_45_sql.replace('i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)',
                                                f'i_item_sk in ({", ".join([convert_id_for_sql(i) for i in [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]])})')
        queries[query_45_index] = (query_45_name, query_45_sql)

        return queries

    def empty(self) -> bool:
        return self.scale == 0


class TPCDSDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "tpcds"

    @staticmethod
    def get_description() -> str:
        return "TPC-DS Benchmark"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-s", "--scale", dest="scale", type=int, default=1, help="scale factor (default: 1)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return TPCDS(base_dir, args, included_queries, excluded_queries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    TPCDSDescription.add_arguments(parser)
    args = parser.parse_args()

    benchmark_instance = TPCDSDescription.instantiate("", vars(args))
    benchmark_instance.queries('duckdb')
