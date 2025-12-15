import argparse
import decimal
import os
import pathlib

from benchmarks import benchmark


class TPCHUUID(benchmark.Benchmark):
    """TPC-H benchmark variant with UUID strings (32 character hex) instead of integer keys."""

    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.scale = args["scale"]
        self.zipf = args["zipf"] if "zipf" in args.keys() else 0

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "tpch_uuid"

    @property
    def queries_path(self) -> str:
        # Reuse queries from standard TPC-H benchmark
        tpch_path = pathlib.Path(__file__).parent.parent / "tpch"
        return os.path.join(tpch_path, "queries" + ("" if self.query_dir is None else f"_{self.query_dir}"))

    @property
    def description(self) -> str:
        return "TPC-H Benchmark with UUID String Keys"

    @property
    def unique_name(self) -> str:
        return f"tpchUuidSf{self.scale}" + ("" if self.zipf == 0 else f"Skew{self.zipf}")

    @property
    def data_dir(self) -> str:
        return os.path.join("tpch_uuid", f"sf{self.scale}" if self.zipf == 0 else f"sf{self.scale}skew{self.zipf}")

    def dbgen(self):
        script_name = f'dbgen{"" if self.zipf == 0 else "Skewed"}.sh'
        script_path = os.path.join(self.path, script_name)
        command = f'{script_path} {self.scale}{"" if self.zipf == 0 else " " + str(self.zipf)}'
        self._load_with_command(command)

    def empty(self) -> bool:
        return self.scale == 0


class TPCHUUIDDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "tpch_uuid"

    @staticmethod
    def get_description() -> str:
        return "TPC-H Benchmark with UUID String Keys"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-s", "--scale", dest="scale", type=decimal.Decimal, default=1, help="scale factor (default: 1)")
        parser.add_argument("-z", "--zipf", dest="zipf", type=decimal.Decimal, default=0, help="zipfian skew (default: 0)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return TPCHUUID(base_dir, args, included_queries, excluded_queries)
