import argparse
import os
import pathlib
from abc import ABC, abstractmethod
from typing import Optional

import natsort
from util import schemajson, logger
from util.process import Process


class Benchmark(ABC):

    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        self._base_dir = base_dir
        self._included_queries = included_queries
        self._excluded_queries = excluded_queries

        self.query_dir = args.get("query_dir", None)

    @property
    @abstractmethod
    def path(self) -> pathlib.Path:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        pass

    @property
    @abstractmethod
    def unique_name(self) -> str:
        pass

    @property
    def result_name(self) -> str:
        return self.unique_name + ("" if self.query_dir is None else f"_{self.query_dir}")

    @property
    @abstractmethod
    def data_dir(self) -> str:
        pass

    @property
    def queries_path(self) -> str:
        return os.path.join(self.path, "queries" + ("" if self.query_dir is None else f"_{self.query_dir}"))

    def _load_with_command(self, command):
        if not os.path.isdir(os.path.join("data", self.data_dir)):
            logger.log_verbose_benchmark(f'Executing dbgen command `{command}`', self)
            with Process(command) as process:
                process.wait()

    @abstractmethod
    def dbgen(self):
        pass

    def empty(self) -> bool:
        return False

    def get_schema(self, primary_key: bool = True, foreign_keys: bool = False, path: Optional[str] = None) -> dict:
        if path is None:
            path = os.path.join(self.path, self.name + '.dbschema.json')
        schema = schemajson.load(path, "dbschema.schema.json")
        for table in schema["tables"]:
            table["file"] = os.path.join(self.data_dir, table["name"] + '.' + schema["file_ending"])
            if "_eval" in table:
                table["_eval"] = eval(table["_eval"], {"dataset": self})

            for column in table["columns"]:
                if "_eval" in column:
                    column["_eval"] = eval(column["_eval"], {"dataset": self})

            if not primary_key and "primary key" in table:
                del table["primary key"]

            if not foreign_keys and "foreign keys" in table:
                table["foreign keys"].clear()

        return schema


    @abstractmethod
    def post_process_queries(self, queries: list[tuple[str, str]]) -> list[tuple[str, str]]:
        return queries


    def queries(self, dbms_name: str) -> list[tuple[str, str]]:
        result = []

        dbms_name_map = {
            "umbradev": "umbra",
            "apollo": "sqlserver"
        }
        dbms_name = dbms_name_map.get(dbms_name, dbms_name)

        for query_file in natsort.natsorted(os.listdir(self.queries_path)):

            # Skip non-SQL files
            if not query_file.endswith(".sql"):
                continue

            # Skip queries that are not included or explicitly excluded
            if self._included_queries and query_file not in self._included_queries:
                continue
            if self._excluded_queries and query_file in self._excluded_queries:
                continue

            query_path = os.path.join(self.queries_path, query_file)
            query = open(query_path, "r").read().strip()

            # Use a specialized variant of the query, if available
            specialized_query_path = query_path + "." + dbms_name
            if os.path.isfile(specialized_query_path):
                query = open(specialized_query_path, "r").read().strip()

            result.append((query_file, query))

        return self.post_process_queries(result)


class BenchmarkDescription:
    @staticmethod
    def get_name() -> str:
        raise NotImplementedError()

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        parser.add_argument("--query-dir", dest="query_dir", type=str, default=None, help="use other queries (name must be of the form `queries_<name>` and the directory must be in the benchmark directory)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> Benchmark:
        raise NotImplementedError()


def benchmarks() -> dict[str, BenchmarkDescription]:
    from benchmarks.clickbench import clickbench
    from benchmarks.job import job
    from benchmarks.ssb import ssb
    from benchmarks.tpcds import tpcds
    from benchmarks.tpch import tpch
    from benchmarks.stackoverflow import stackoverflow

    benchmark_list = [
        clickbench.ClickBenchDescription,
        job.JOBDescription,
        ssb.SSBDescription,
        stackoverflow.StackOverflowDescription,
        tpcds.TPCDSDescription,
        tpch.TPCHDescription,
    ]
    return {benchmark.get_name(): benchmark for benchmark in benchmark_list}


def benchmark_arguments(parser: argparse.ArgumentParser):
    benchmark_map = benchmarks()

    benchmark_parsers = parser.add_subparsers(dest="benchmark", required=True)
    for name, benchmark in benchmark_map.items():
        benchmark_parser = benchmark_parsers.add_parser(name, help=benchmark.get_description())
        benchmark.add_arguments(benchmark_parser)

    benchmark_parsers.add_parser("default", help="use the default benchmarks")

    return benchmark_map
