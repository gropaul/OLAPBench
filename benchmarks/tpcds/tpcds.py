import argparse
import os
import pathlib

from benchmarks import benchmark
from benchmarks.tpch.utils import create_string_id_data, TPC_DS_TABLE_ID_COLUMNS, TPC_ID_TYPE


class TPCDS(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.scale = args["scale"]
        self.id_type: TPC_ID_TYPE = args["id_type"] if "id_type" in args.keys() else "int64_sorted"

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "tpcds"

    @property
    def description(self) -> str:
        return "TPC-DS Benchmark"

    @property
    def unique_name(self) -> str:
        return f"tpcdsSf{self.scale}"

    @property
    def data_dir(self) -> str:
        return os.path.join("tpcds", f"sf{self.scale}")

    def dbgen(self):
        self._load_with_command(f'{os.path.join(self.path, "dbgen.sh")} {self.scale}')
        self._post_process()

    def _post_process(self):
        create_string_id_data(self, 'benchmarks/tpcds/tpcds.dbschema.json', TPC_DS_TABLE_ID_COLUMNS)

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
    benchmark_instance.dbgen()
