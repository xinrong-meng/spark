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
import os
import time
import unittest

from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)

if have_pyarrow:
    import pyarrow as pa

if have_pandas:
    import pandas as pd


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class MapInArrowTests(ReusedSQLTestCase):
    # dummy dummy
    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        ReusedSQLTestCase.tearDownClass()

    def test_map_in_arrow(self):
        def func(iterator):
            for batch in iterator:
                assert isinstance(batch, pa.RecordBatch)
                assert batch.schema.names == ["id"]
                yield batch

        df = self.spark.range(10)
        actual = df.mapInArrow(func, "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_multiple_columns(self):
        data = [(1, "foo"), (2, None), (3, "bar"), (4, "bar")]
        df = self.spark.createDataFrame(data, "a int, b string")

        def func(iterator):
            for batch in iterator:
                assert isinstance(batch, pa.RecordBatch)
                assert batch.schema.types == [pa.int32(), pa.string()]
                yield batch

        actual = df.mapInArrow(func, df.schema).collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_different_output_length(self):
        def func(iterator):
            for _ in iterator:
                yield pa.RecordBatch.from_pandas(pd.DataFrame({"a": list(range(100))}))

        df = self.spark.range(10)
        actual = df.repartition(1).mapInArrow(func, "a long").collect()
        self.assertEqual(set((r.a for r in actual)), set(range(100)))

    def test_empty_iterator(self):
        def empty_iter(_):
            return iter([])

        self.assertEqual(self.spark.range(10).mapInArrow(empty_iter, "a int, b string").count(), 0)

    def test_empty_rows(self):
        def empty_rows(_):
            return iter([pa.RecordBatch.from_pandas(pd.DataFrame({"a": []}))])

        self.assertEqual(self.spark.range(10).mapInArrow(empty_rows, "a int").count(), 0)

    def test_chain_map_in_arrow(self):
        def func(iterator):
            for batch in iterator:
                assert isinstance(batch, pa.RecordBatch)
                assert batch.schema.names == ["id"]
                yield batch

        df = self.spark.range(10)
        actual = df.mapInArrow(func, "id long").mapInArrow(func, "id long").collect()
        expected = df.collect()
        self.assertEqual(actual, expected)

    def test_self_join(self):
        df1 = self.spark.range(10)
        df2 = df1.mapInArrow(lambda iter: iter, "id long")
        actual = df2.join(df2).collect()
        expected = df1.join(df1).collect()
        self.assertEqual(sorted(actual), sorted(expected))


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow_map import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
