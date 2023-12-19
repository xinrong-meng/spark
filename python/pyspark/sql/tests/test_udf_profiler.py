#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from contextlib import contextmanager
import inspect
import tempfile
import unittest
import os
import sys
from io import StringIO

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.profiler import UDFBasicProfiler
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import eventually


def _do_computation(spark, *, action=lambda df: df.collect(), use_arrow=False):
    @udf("long", useArrow=use_arrow)
    def add1(x):
        return x + 1

    @udf("long", useArrow=use_arrow)
    def add2(x):
        return x + 2

    df = spark.range(10).select(add1("id"), add2("id"), add1("id"), add2(col("id") + 1))
    action(df)


class UDFProfilerTests(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile", "true")
        self.spark = (
            SparkSession.builder.master("local[4]")
            .config(conf=conf)
            .appName(class_name)
            .getOrCreate()
        )
        self.sc = self.spark.sparkContext

    def tearDown(self):
        self.spark.stop()
        sys.path = self._old_sys_path

    def test_udf_profiler(self):
        _do_computation(self.spark)

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(4, len(profilers))

        old_stdout = sys.stdout
        try:
            sys.stdout = io = StringIO()
            self.sc.show_profiles()
        finally:
            sys.stdout = old_stdout

        d = tempfile.gettempdir()
        self.sc.dump_profiles(d)

        for i, udf_name in enumerate(["add1", "add2", "add1", "add2"]):
            id, profiler, _ = profilers[i]
            with self.subTest(id=id, udf_name=udf_name):
                stats = profiler.stats()
                self.assertTrue(stats is not None)
                width, stat_list = stats.get_print_list([])
                func_names = [func_name for fname, n, func_name in stat_list]
                self.assertTrue(udf_name in func_names)

                self.assertTrue(udf_name in io.getvalue())
                self.assertTrue("udf_%d.pstats" % id in os.listdir(d))

    def test_custom_udf_profiler(self):
        class TestCustomProfiler(UDFBasicProfiler):
            def show(self, id):
                self.result = "Custom formatting"

        self.sc.profiler_collector.udf_profiler_cls = TestCustomProfiler

        _do_computation(self.spark)

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(4, len(profilers))
        _, profiler, _ = profilers[0]
        self.assertTrue(isinstance(profiler, TestCustomProfiler))

        self.sc.show_profiles()
        self.assertEqual("Custom formatting", profiler.result)


class UDFProfiler2TestsMixin:
    def setUp(self) -> None:
        super().setUp()
        self.spark._profiler_collector._clear()

    @contextmanager
    def trap_stdout(self):
        old_stdout = sys.stdout
        sys.stdout = io = StringIO()
        try:
            yield io
        finally:
            sys.stdout = old_stdout

    def test_perf_profiler_udf(self):
        _do_computation(self.spark)

        profile_results = self.spark._profiler_collector._perf_profile_results

        # Without the conf enabled, no profile results are collected.
        self.assertEqual(0, len(profile_results), str(list(profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)

        def check():
            self.assertEqual(3, len(profile_results), str(list(profile_results)))

            with self.trap_stdout() as io_all:
                self.spark.show_perf_profiles()

            for id in profile_results:
                self.assertIn(f"Profile of UDF<id={id}>", io_all.getvalue())

                with self.trap_stdout() as io:
                    self.spark.show_perf_profiles(id)

                self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
                self.assertRegex(
                    io.getvalue(), f"10.*{os.path.basename(inspect.getfile(_do_computation))}"
                )

        eventually(catch_assertions=True)(check)()

    def test_perf_profiler_udf_with_arrow(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark, use_arrow=True)

        profile_results = self.spark._profiler_collector._perf_profile_results

        def check():
            self.assertEqual(3, len(profile_results), str(list(profile_results)))

            for id in profile_results:
                with self.trap_stdout() as io:
                    self.spark.show_perf_profiles(id)

                self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
                self.assertRegex(
                    io.getvalue(), f"10.*{os.path.basename(inspect.getfile(_do_computation))}"
                )

        eventually(catch_assertions=True)(check)()

    def test_perf_profiler_udf_multiple_actions(self):
        def action(df):
            df.collect()
            df.show()

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark, action=action)

        profile_results = self.spark._profiler_collector._perf_profile_results

        def check():
            self.assertEqual(3, len(profile_results), str(list(profile_results)))

            for id in profile_results:
                with self.trap_stdout() as io:
                    self.spark.show_perf_profiles(id)

                self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
                self.assertRegex(
                    io.getvalue(), f"20.*{os.path.basename(inspect.getfile(_do_computation))}"
                )

        eventually(catch_assertions=True)(check)()

    def test_perf_profiler_udf_registered(self):
        @udf("long")
        def add1(x):
            return x + 1

        self.spark.udf.register("add1", add1)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            self.spark.sql("SELECT id, add1(id) add1 FROM range(10)").collect()

        profile_results = self.spark._profiler_collector._perf_profile_results

        def check():
            self.assertEqual(1, len(profile_results), str(profile_results.keys()))

            for id in profile_results:
                with self.trap_stdout() as io:
                    self.spark.show_perf_profiles(id)

                self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
                self.assertRegex(
                    io.getvalue(), f"10.*{os.path.basename(inspect.getfile(_do_computation))}"
                )

        eventually(catch_assertions=True)(check)()

    def test_perf_profiler_pandas_udf(self):
        @pandas_udf("long")
        def add1(x):
            return x + 1

        @pandas_udf("long")
        def add2(x):
            return x + 2

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df = self.spark.range(10, numPartitions=2).select(
                add1("id"), add2("id"), add1("id"), add2(col("id") + 1)
            )
            df.collect()

        profile_results = self.spark._profiler_collector._perf_profile_results

        def check():
            self.assertEqual(3, len(profile_results), str(profile_results.keys()))

            for id in profile_results:
                with self.trap_stdout() as io:
                    self.spark.show_perf_profiles(id)

                self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
                self.assertRegex(
                    io.getvalue(), f"2.*{os.path.basename(inspect.getfile(_do_computation))}"
                )

        eventually(catch_assertions=True)(check)()


class UDFProfiler2Tests(UDFProfiler2TestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_udf_profiler import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
