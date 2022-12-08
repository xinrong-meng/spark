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
from typing import Any
import unittest
import tempfile

from pyspark.testing.sqlutils import have_pandas, SQLTestUtils

from pyspark.sql import SparkSession

if have_pandas:
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.testing.pandasutils import PandasOnSparkTestCase
else:
    from pyspark.testing.sqlutils import ReusedSQLTestCase as PandasOnSparkTestCase  # type: ignore
from pyspark.sql.dataframe import DataFrame
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import ReusedPySparkTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectFuncTestCase(PandasOnSparkTestCase, ReusedPySparkTestCase, SQLTestUtils):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    if have_pandas:
        connect: RemoteSparkSession
    tbl_name: str
    tbl_name_empty: str
    df_text: "DataFrame"
    spark: SparkSession

    @classmethod
    def setUpClass(cls: Any):
        ReusedPySparkTestCase.setUpClass()
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        cls.hive_available = True
        # Create the new Spark Session
        cls.spark = SparkSession(cls.sc)
        # Setup Remote Spark Session
        cls.connect = RemoteSparkSession.builder.remote().getOrCreate()

    @classmethod
    def tearDownClass(cls: Any) -> None:
        ReusedPySparkTestCase.tearDownClass()


class SparkConnectFunctionTests(SparkConnectFuncTestCase):
    """These test cases exercise the interface to the proto plan
    generation but do not call Spark."""

    def compare_by_show(self, df1: Any, df2: Any):
        from pyspark.sql.dataframe import DataFrame as SDF
        from pyspark.sql.connect.dataframe import DataFrame as CDF

        assert isinstance(df1, (SDF, CDF))
        if isinstance(df1, SDF):
            str1 = df1._jdf.showString(20, 20, False)
        else:
            str1 = df1._show_string(20, 20, False)

        assert isinstance(df2, (SDF, CDF))
        if isinstance(df2, SDF):
            str2 = df2._jdf.showString(20, 20, False)
        else:
            str2 = df2._show_string(20, 20, False)

        self.assertEqual(str1, str2)

    def test_normal_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (0, float("NAN"), NULL), (1, NULL, 2.0), (2, 2.1, 3.5)
            AS tab(a, b, c)
            """
        # +---+----+----+
        # |  a|   b|   c|
        # +---+----+----+
        # |  0| NaN|null|
        # |  1|null| 2.0|
        # |  2| 2.1| 3.5|
        # +---+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        self.assert_eq(
            cdf.select(CF.bitwise_not(cdf.a)).toPandas(),
            sdf.select(SF.bitwise_not(sdf.a)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.coalesce(cdf.a, "b", cdf.c)).toPandas(),
            sdf.select(SF.coalesce(sdf.a, "b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.expr("a + b - c")).toPandas(),
            sdf.select(SF.expr("a + b - c")).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.greatest(cdf.a, "b", cdf.c)).toPandas(),
            sdf.select(SF.greatest(sdf.a, "b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.isnan(cdf.a), CF.isnan("b")).toPandas(),
            sdf.select(SF.isnan(sdf.a), SF.isnan("b")).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.isnull(cdf.a), CF.isnull("b")).toPandas(),
            sdf.select(SF.isnull(sdf.a), SF.isnull("b")).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.input_file_name()).toPandas(),
            sdf.select(SF.input_file_name()).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.least(cdf.a, "b", cdf.c)).toPandas(),
            sdf.select(SF.least(sdf.a, "b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.monotonically_increasing_id()).toPandas(),
            sdf.select(SF.monotonically_increasing_id()).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.nanvl("b", cdf.c)).toPandas(),
            sdf.select(SF.nanvl("b", sdf.c)).toPandas(),
        )
        # Can not compare the values due to the random seed
        self.assertEqual(
            cdf.select(CF.rand()).count(),
            sdf.select(SF.rand()).count(),
        )
        self.assert_eq(
            cdf.select(CF.rand(100)).toPandas(),
            sdf.select(SF.rand(100)).toPandas(),
        )
        # Can not compare the values due to the random seed
        self.assertEqual(
            cdf.select(CF.randn()).count(),
            sdf.select(SF.randn()).count(),
        )
        self.assert_eq(
            cdf.select(CF.randn(100)).toPandas(),
            sdf.select(SF.randn(100)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.spark_partition_id()).toPandas(),
            sdf.select(SF.spark_partition_id()).toPandas(),
        )

    def test_sorting_functions_with_column(self):
        from pyspark.sql.connect import functions as CF
        from pyspark.sql.connect.column import Column

        funs = [
            CF.asc_nulls_first,
            CF.asc_nulls_last,
            CF.desc_nulls_first,
            CF.desc_nulls_last,
        ]
        exprs = [CF.col("x"), "x"]

        for fun in funs:
            for _expr in exprs:
                res = fun(_expr)
                self.assertIsInstance(res, Column)
                self.assertIn(f"""{fun.__name__.replace("_", " ").upper()}'""", str(res))

        for _expr in exprs:
            res = CF.asc(_expr)
            self.assertIsInstance(res, Column)
            self.assertIn("""ASC NULLS FIRST'""", str(res))

        for _expr in exprs:
            res = CF.desc(_expr)
            self.assertIsInstance(res, Column)
            self.assertIn("""DESC NULLS LAST'""", str(res))

    def test_sort_with_nulls_order(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (true, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|null|
        # | true|null| 2.0|
        # | null|   3| 3.0|
        # +-----+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        for c in ["a", "b", "c"]:
            self.assert_eq(
                cdf.orderBy(CF.asc(c)).toPandas(),
                sdf.orderBy(SF.asc(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.asc_nulls_first(c)).toPandas(),
                sdf.orderBy(SF.asc_nulls_first(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.asc_nulls_last(c)).toPandas(),
                sdf.orderBy(SF.asc_nulls_last(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.desc(c)).toPandas(),
                sdf.orderBy(SF.desc(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.desc_nulls_first(c)).toPandas(),
                sdf.orderBy(SF.desc_nulls_first(c)).toPandas(),
            )
            self.assert_eq(
                cdf.orderBy(CF.desc_nulls_last(c)).toPandas(),
                sdf.orderBy(SF.desc_nulls_last(c)).toPandas(),
            )

    def test_math_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (true, NULL, 2.0), (NULL, 3, 3.5)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|null|
        # | true|null| 2.0|
        # | null|   3| 3.5|
        # +-----+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        for cfunc, sfunc in [
            (CF.abs, SF.abs),
            (CF.acos, SF.acos),
            (CF.acosh, SF.acosh),
            (CF.asin, SF.asin),
            (CF.asinh, SF.asinh),
            (CF.atan, SF.atan),
            (CF.atanh, SF.atanh),
            (CF.bin, SF.bin),
            (CF.cbrt, SF.cbrt),
            (CF.ceil, SF.ceil),
            (CF.cos, SF.cos),
            (CF.cosh, SF.cosh),
            (CF.cot, SF.cot),
            (CF.csc, SF.csc),
            (CF.degrees, SF.degrees),
            (CF.exp, SF.exp),
            (CF.expm1, SF.expm1),
            (CF.factorial, SF.factorial),
            (CF.floor, SF.floor),
            (CF.hex, SF.hex),
            (CF.log, SF.log),
            (CF.log10, SF.log10),
            (CF.log1p, SF.log1p),
            (CF.log2, SF.log2),
            (CF.radians, SF.radians),
            (CF.rint, SF.rint),
            (CF.sec, SF.sec),
            (CF.signum, SF.signum),
            (CF.sin, SF.sin),
            (CF.sinh, SF.sinh),
            (CF.sqrt, SF.sqrt),
            (CF.tan, SF.tan),
            (CF.tanh, SF.tanh),
            (CF.unhex, SF.unhex),
        ]:
            self.assert_eq(
                cdf.select(cfunc("b"), cfunc(cdf.c)).toPandas(),
                sdf.select(sfunc("b"), sfunc(sdf.c)).toPandas(),
            )

        self.assert_eq(
            cdf.select(CF.atan2("b", cdf.c)).toPandas(),
            sdf.select(SF.atan2("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.bround("b", 1)).toPandas(),
            sdf.select(SF.bround("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.conv("b", 2, 16)).toPandas(),
            sdf.select(SF.conv("b", 2, 16)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.hypot("b", cdf.c)).toPandas(),
            sdf.select(SF.hypot("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.pmod("b", cdf.c)).toPandas(),
            sdf.select(SF.pmod("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.pow("b", cdf.c)).toPandas(),
            sdf.select(SF.pow("b", sdf.c)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.round("b", 1)).toPandas(),
            sdf.select(SF.round("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.shiftleft("b", 1)).toPandas(),
            sdf.select(SF.shiftleft("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.shiftright("b", 1)).toPandas(),
            sdf.select(SF.shiftright("b", 1)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.shiftrightunsigned("b", 1)).toPandas(),
            sdf.select(SF.shiftrightunsigned("b", 1)).toPandas(),
        )

    def test_aggregation_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (0, float("NAN"), NULL), (1, NULL, 2.0), (1, 2.1, 3.5), (0, 0.5, 1.0)
            AS tab(a, b, c)
            """
        # +---+----+----+
        # |  a|   b|   c|
        # +---+----+----+
        # |  0| NaN|null|
        # |  1|null| 2.0|
        # |  1| 2.1| 3.5|
        # |  0| 0.5| 1.0|
        # +---+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # TODO(SPARK-41383): add tests for grouping, grouping_id after DataFrame.cube is supported.
        for cfunc, sfunc in [
            (CF.approx_count_distinct, SF.approx_count_distinct),
            (CF.avg, SF.avg),
            (CF.collect_list, SF.collect_list),
            (CF.collect_set, SF.collect_set),
            (CF.count, SF.count),
            (CF.first, SF.first),
            (CF.kurtosis, SF.kurtosis),
            (CF.last, SF.last),
            (CF.max, SF.max),
            (CF.mean, SF.mean),
            (CF.median, SF.median),
            (CF.min, SF.min),
            (CF.mode, SF.mode),
            (CF.product, SF.product),
            (CF.skewness, SF.skewness),
            (CF.stddev, SF.stddev),
            (CF.stddev_pop, SF.stddev_pop),
            (CF.stddev_samp, SF.stddev_samp),
            (CF.sum, SF.sum),
            (CF.sum_distinct, SF.sum_distinct),
            (CF.var_pop, SF.var_pop),
            (CF.var_samp, SF.var_samp),
            (CF.variance, SF.variance),
        ]:
            self.assert_eq(
                cdf.select(cfunc("b"), cfunc(cdf.c)).toPandas(),
                sdf.select(sfunc("b"), sfunc(sdf.c)).toPandas(),
            )
            self.assert_eq(
                cdf.groupBy("a").agg(cfunc("b"), cfunc(cdf.c)).toPandas(),
                sdf.groupBy("a").agg(sfunc("b"), sfunc(sdf.c)).toPandas(),
            )

        for cfunc, sfunc in [
            (CF.corr, SF.corr),
            (CF.covar_pop, SF.covar_pop),
            (CF.covar_samp, SF.covar_samp),
            (CF.max_by, SF.max_by),
            (CF.min_by, SF.min_by),
        ]:
            self.assert_eq(
                cdf.select(cfunc(cdf.b, "c")).toPandas(),
                sdf.select(sfunc(sdf.b, "c")).toPandas(),
            )
            self.assert_eq(
                cdf.groupBy("a").agg(cfunc(cdf.b, "c")).toPandas(),
                sdf.groupBy("a").agg(sfunc(sdf.b, "c")).toPandas(),
            )

        # test percentile_approx
        self.assert_eq(
            cdf.select(CF.percentile_approx(cdf.b, 0.5, 1000)).toPandas(),
            sdf.select(SF.percentile_approx(sdf.b, 0.5, 1000)).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.percentile_approx(cdf.b, [0.1, 0.9])).toPandas(),
            sdf.select(SF.percentile_approx(sdf.b, [0.1, 0.9])).toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("a").agg(CF.percentile_approx("b", 0.5)).toPandas(),
            sdf.groupBy("a").agg(SF.percentile_approx("b", 0.5)).toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("a").agg(CF.percentile_approx(cdf.b, [0.1, 0.9])).toPandas(),
            sdf.groupBy("a").agg(SF.percentile_approx(sdf.b, [0.1, 0.9])).toPandas(),
        )

        # test count_distinct
        self.assert_eq(
            cdf.select(CF.count_distinct("b"), CF.count_distinct(cdf.c)).toPandas(),
            sdf.select(SF.count_distinct("b"), SF.count_distinct(sdf.c)).toPandas(),
        )
        # The output column names of 'groupBy.agg(count_distinct)' in PySpark
        # are incorrect, see SPARK-41391.
        self.assert_eq(
            cdf.groupBy("a")
            .agg(CF.count_distinct("b").alias("x"), CF.count_distinct(cdf.c).alias("y"))
            .toPandas(),
            sdf.groupBy("a")
            .agg(SF.count_distinct("b").alias("x"), SF.count_distinct(sdf.c).alias("y"))
            .toPandas(),
        )

    def test_collection_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            (ARRAY('a', 'ab'), ARRAY(1, 2, 3), ARRAY(1, NULL, 3), 1, 2, 'a'),
            (ARRAY('x', NULL), NULL, ARRAY(1, 3), 3, 4, 'x'),
            (NULL, ARRAY(-1, -2, -3), Array(), 5, 6, NULL)
            AS tab(a, b, c, d, e, f)
            """
        # +---------+------------+------------+---+---+----+
        # |        a|           b|           c|  d|  e|   f|
        # +---------+------------+------------+---+---+----+
        # |  [a, ab]|   [1, 2, 3]|[1, null, 3]|  1|  2|   a|
        # |[x, null]|        null|      [1, 3]|  3|  4|   x|
        # |     null|[-1, -2, -3]|          []|  5|  6|null|
        # +---------+------------+------------+---+---+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        for cfunc, sfunc in [
            (CF.array_distinct, SF.array_distinct),
            (CF.array_max, SF.array_max),
            (CF.array_min, SF.array_min),
        ]:
            self.assert_eq(
                cdf.select(cfunc("a"), cfunc(cdf.b)).toPandas(),
                sdf.select(sfunc("a"), sfunc(sdf.b)).toPandas(),
            )

        for cfunc, sfunc in [
            (CF.array_except, SF.array_except),
            (CF.array_intersect, SF.array_intersect),
            (CF.array_union, SF.array_union),
            (CF.arrays_overlap, SF.arrays_overlap),
        ]:
            self.assert_eq(
                cdf.select(cfunc("b", cdf.c)).toPandas(),
                sdf.select(sfunc("b", sdf.c)).toPandas(),
            )

        for cfunc, sfunc in [
            (CF.array_position, SF.array_position),
            (CF.array_remove, SF.array_remove),
        ]:
            self.assert_eq(
                cdf.select(cfunc(cdf.a, "ab")).toPandas(),
                sdf.select(sfunc(sdf.a, "ab")).toPandas(),
            )

        # test array
        self.assert_eq(
            cdf.select(CF.array(cdf.d, "e")).toPandas(),
            sdf.select(SF.array(sdf.d, "e")).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.array(cdf.d, "e", CF.lit(99))).toPandas(),
            sdf.select(SF.array(sdf.d, "e", SF.lit(99))).toPandas(),
        )

        # test array_contains
        self.assert_eq(
            cdf.select(CF.array_contains(cdf.a, "ab")).toPandas(),
            sdf.select(SF.array_contains(sdf.a, "ab")).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.array_contains(cdf.a, cdf.f)).toPandas(),
            sdf.select(SF.array_contains(sdf.a, sdf.f)).toPandas(),
        )

        # test array_join
        self.assert_eq(
            cdf.select(
                CF.array_join(cdf.a, ","), CF.array_join("b", ":"), CF.array_join("c", "~")
            ).toPandas(),
            sdf.select(
                SF.array_join(sdf.a, ","), SF.array_join("b", ":"), SF.array_join("c", "~")
            ).toPandas(),
        )
        self.assert_eq(
            cdf.select(
                CF.array_join(cdf.a, ",", "_null_"),
                CF.array_join("b", ":", ".null."),
                CF.array_join("c", "~", "NULL"),
            ).toPandas(),
            sdf.select(
                SF.array_join(sdf.a, ",", "_null_"),
                SF.array_join("b", ":", ".null."),
                SF.array_join("c", "~", "NULL"),
            ).toPandas(),
        )

        # test array_repeat
        self.assert_eq(
            cdf.select(CF.array_repeat(cdf.f, "d")).toPandas(),
            sdf.select(SF.array_repeat(sdf.f, "d")).toPandas(),
        )
        self.assert_eq(
            cdf.select(CF.array_repeat("f", cdf.d)).toPandas(),
            sdf.select(SF.array_repeat("f", sdf.d)).toPandas(),
        )
        # TODO: Make Literal contains DataType
        #   Cannot resolve "array_repeat(f, 3)" due to data type mismatch:
        #   Parameter 2 requires the "INT" type, however "3" has the type "BIGINT".
        # self.assert_eq(
        #     cdf.select(CF.array_repeat("f", 3)).toPandas(),
        #     sdf.select(SF.array_repeat("f", 3)).toPandas(),
        # )

        # test arrays_zip
        # TODO: Make toPandas support complex nested types like Array<Struct>
        # DataFrame.iloc[:, 0] (column name="arrays_zip(b, c)") values are different (66.66667 %)
        # [index]: [0, 1, 2]
        # [left]:  [[{'b': 1, 'c': 1.0}, {'b': 2, 'c': None}, {'b': 3, 'c': 3.0}], None,
        #           [{'b': -1, 'c': None}, {'b': -2, 'c': None}, {'b': -3, 'c': None}]]
        # [right]: [[(1, 1), (2, None), (3, 3)], None, [(-1, None), (-2, None), (-3, None)]]
        self.compare_by_show(
            cdf.select(CF.arrays_zip(cdf.b, "c")),
            sdf.select(SF.arrays_zip(sdf.b, "c")),
        )

        # test concat
        self.assert_eq(
            cdf.select(CF.concat("d", cdf.e, CF.lit(-1))).toPandas(),
            sdf.select(SF.concat("d", sdf.e, SF.lit(-1))).toPandas(),
        )

        # test create_map
        self.compare_by_show(
            cdf.select(CF.create_map(cdf.d, cdf.e)), sdf.select(SF.create_map(sdf.d, sdf.e))
        )
        self.compare_by_show(
            cdf.select(CF.create_map(cdf.d, "e", "e", CF.lit(1))),
            sdf.select(SF.create_map(sdf.d, "e", "e", SF.lit(1))),
        )

    def test_string_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            ('   ab   ', 'ab   ', NULL), ('   ab', NULL, 'ab')
            AS tab(a, b, c)
            """
        # +--------+-----+----+
        # |       a|    b|   c|
        # +--------+-----+----+
        # |   ab   |ab   |null|
        # |      ab| null|  ab|
        # +--------+-----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        for cfunc, sfunc in [
            (CF.upper, SF.upper),
            (CF.lower, SF.lower),
            (CF.ascii, SF.ascii),
            (CF.base64, SF.base64),
            (CF.unbase64, SF.unbase64),
            (CF.ltrim, SF.ltrim),
            (CF.rtrim, SF.rtrim),
            (CF.trim, SF.trim),
        ]:
            self.assert_eq(
                cdf.select(cfunc("a"), cfunc(cdf.b)).toPandas(),
                sdf.select(sfunc("a"), sfunc(sdf.b)).toPandas(),
            )

        self.assert_eq(
            cdf.select(CF.concat_ws("-", cdf.a, "c")).toPandas(),
            sdf.select(SF.concat_ws("-", sdf.a, "c")).toPandas(),
        )

        # Disable the test for "decode" because of inconsistent column names,
        # as shown below
        #
        # >>> sdf.select(SF.decode("c", "UTF-8")).toPandas()
        # stringdecode(c, UTF-8)
        # 0                   None
        # 1                     ab
        # >>> cdf.select(CF.decode("c", "UTF-8")).toPandas()
        # decode(c, UTF-8)
        # 0             None
        # 1               ab
        #
        # self.assert_eq(
        #     cdf.select(CF.decode("c", "UTF-8")).toPandas(),
        #     sdf.select(SF.decode("c", "UTF-8")).toPandas(),
        # )

        self.assert_eq(
            cdf.select(CF.encode("c", "UTF-8")).toPandas(),
            sdf.select(SF.encode("c", "UTF-8")).toPandas(),
        )

    # TODO(SPARK-41283): To compare toPandas for test cases with dtypes marked
    def test_date_ts_functions(self):
        from pyspark.sql import functions as SF
        from pyspark.sql.connect import functions as CF

        query = """
            SELECT * FROM VALUES
            ('1997/02/28 10:30:00', '2023/03/01 06:00:00', 'JST', 1428476400, 2020, 12, 6),
            ('2000/01/01 04:30:05', '2020/05/01 12:15:00', 'PST', 1403892395, 2022, 12, 6)
            AS tab(ts1, ts2, tz, seconds, Y, M, D)
            """
        # +-------------------+-------------------+---+----------+----+---+---+
        # |                ts1|                ts2| tz|   seconds|   Y|  M|  D|
        # +-------------------+-------------------+---+----------+----+---+---+
        # |1997/02/28 10:30:00|2023/03/01 06:00:00|JST|1428476400|2020| 12|  6|
        # |2000/01/01 04:30:05|2020/05/01 12:15:00|PST|1403892395|2022| 12|  6|
        # +-------------------+-------------------+---+----------+----+---+---+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # With no parameters
        for cfunc, sfunc in [
            (CF.current_date, SF.current_date),
        ]:
            self.assert_eq(
                cdf.select(cfunc()).toPandas(),
                sdf.select(sfunc()).toPandas(),
            )

        # current_timestamp
        # [left]:  datetime64[ns, America/Los_Angeles]
        # [right]: datetime64[ns]
        plan = cdf.select(CF.current_timestamp())._plan.to_proto(self.connect)
        self.assertEqual(
            plan.root.project.expressions.unresolved_function.function_name, "current_timestamp"
        )

        # localtimestamp
        plan = cdf.select(CF.localtimestamp())._plan.to_proto(self.connect)
        self.assertEqual(
            plan.root.project.expressions.unresolved_function.function_name, "localtimestamp"
        )

        # With only column parameter
        for cfunc, sfunc in [
            (CF.year, SF.year),
            (CF.quarter, SF.quarter),
            (CF.month, SF.month),
            (CF.dayofweek, SF.dayofweek),
            (CF.dayofmonth, SF.dayofmonth),
            (CF.dayofyear, SF.dayofyear),
            (CF.hour, SF.hour),
            (CF.minute, SF.minute),
            (CF.second, SF.second),
            (CF.weekofyear, SF.weekofyear),
            (CF.last_day, SF.last_day),
            (CF.unix_timestamp, SF.unix_timestamp),
        ]:
            self.assert_eq(
                cdf.select(cfunc(cdf.ts1)).toPandas(),
                sdf.select(sfunc(sdf.ts1)).toPandas(),
            )

        # With format parameter
        for cfunc, sfunc in [
            (CF.date_format, SF.date_format),
            (CF.to_date, SF.to_date),
        ]:
            self.assert_eq(
                cdf.select(cfunc(cdf.ts1, format="yyyy-MM-dd")).toPandas(),
                sdf.select(sfunc(sdf.ts1, format="yyyy-MM-dd")).toPandas(),
            )
        self.compare_by_show(
            # [left]:  datetime64[ns, America/Los_Angeles]
            # [right]: datetime64[ns]
            cdf.select(CF.to_timestamp(cdf.ts1, format="yyyy-MM-dd")),
            sdf.select(SF.to_timestamp(sdf.ts1, format="yyyy-MM-dd")),
        )

        # With tz parameter
        for cfunc, sfunc in [
            (CF.from_utc_timestamp, SF.from_utc_timestamp),
            (CF.to_utc_timestamp, SF.to_utc_timestamp),
            # [left]:  datetime64[ns, America/Los_Angeles]
            # [right]: datetime64[ns]
        ]:
            self.compare_by_show(
                cdf.select(cfunc(cdf.ts1, tz=cdf.tz)),
                sdf.select(sfunc(sdf.ts1, tz=sdf.tz)),
            )

        # With numeric parameter
        for cfunc, sfunc in [
            (CF.date_add, SF.date_add),
            (CF.date_sub, SF.date_sub),
            (CF.add_months, SF.add_months),
        ]:
            self.assert_eq(
                cdf.select(cfunc(cdf.ts1, cdf.D)).toPandas(),
                sdf.select(sfunc(sdf.ts1, sdf.D)).toPandas(),
            )

        # With another timestamp as parameter
        for cfunc, sfunc in [
            (CF.datediff, SF.datediff),
            (CF.months_between, SF.months_between),
        ]:
            self.assert_eq(
                cdf.select(cfunc(cdf.ts1, cdf.ts2)).toPandas(),
                sdf.select(sfunc(sdf.ts1, sdf.ts2)).toPandas(),
            )

        # With seconds parameter
        self.compare_by_show(
            # [left]:  datetime64[ns, America/Los_Angeles]
            # [right]: datetime64[ns]
            cdf.select(CF.timestamp_seconds(cdf.seconds)),
            sdf.select(SF.timestamp_seconds(sdf.seconds)),
        )

        # make_date
        self.assert_eq(
            cdf.select(CF.make_date(cdf.Y, cdf.M, cdf.D)).toPandas(),
            sdf.select(SF.make_date(sdf.Y, sdf.M, sdf.D)).toPandas(),
        )

        # date_trunc
        self.assert_eq(
            # [left]:  datetime64[ns, America/Los_Angeles]
            # [right]: datetime64[ns]
            cdf.select(CF.date_trunc("day", cdf.ts1)),
            sdf.select(SF.date_trunc("day", sdf.ts1)),
        )

        # trunc
        self.assert_eq(
            cdf.select(CF.trunc(cdf.ts1, "year")).toPandas(),
            sdf.select(SF.trunc(sdf.ts1, "year")).toPandas(),
        )

        # next_day
        self.assert_eq(
            cdf.select(CF.next_day(cdf.ts1, "Mon")).toPandas(),
            sdf.select(SF.next_day(sdf.ts1, "Mon")).toPandas(),
        )


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_function import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
