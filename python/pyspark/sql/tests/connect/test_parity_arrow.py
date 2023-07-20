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

import unittest
from distutils.version import LooseVersion

import pandas as pd

from pyspark.sql.tests.test_arrow import ArrowTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils
from pyspark.sql.connect.functions import rand


class ArrowParityTests(ArrowTestsMixin, ReusedConnectTestCase, PandasOnSparkTestUtils):
    @unittest.skip("Spark Connect does not support Spark Context but the test depends on that.")
    def test_createDataFrame_empty_partition(self):
        super().test_createDataFrame_empty_partition()

    @unittest.skip("Spark Connect does not support fallback.")
    def test_createDataFrame_fallback_disabled(self):
        super().test_createDataFrame_fallback_disabled()

    @unittest.skip("Spark Connect does not support fallback.")
    def test_createDataFrame_fallback_enabled(self):
        super().test_createDataFrame_fallback_enabled()

    def test_createDataFrame_with_incorrect_schema(self):
        self.check_createDataFrame_with_incorrect_schema()

    def test_createDataFrame_with_map_type(self):
        self.check_createDataFrame_with_map_type(True)

    def test_createDataFrame_with_ndarray(self):
        self.check_createDataFrame_with_ndarray(True)

    def test_createDataFrame_with_single_data_type(self):
        self.check_createDataFrame_with_single_data_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_no_partition_frame(self):
        super().test_no_partition_frame()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_no_partition_toPandas(self):
        super().test_no_partition_toPandas()

    def test_pandas_self_destruct(self):
        df = self.spark.range(100).select('id', 'id', 'id')

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.selfDestruct.enabled": True}):
            self_destruct_pdf = df.toPandas()

        with self.sql_conf({"spark.sql.execution.arrow.pyspark.selfDestruct.enabled": False}):
            no_self_destruct_pdf = df.toPandas()

        self.assert_eq(self_destruct_pdf, no_self_destruct_pdf)

    def test_propagates_spark_exception(self):
        self.check_propagates_spark_exception()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_toPandas_batch_order(self):
        super().test_toPandas_batch_order()

    def test_toPandas_empty_df_arrow_enabled(self):
        self.check_toPandas_empty_df_arrow_enabled(True)

    def test_create_data_frame_to_pandas_timestamp_ntz(self):
        self.check_create_data_frame_to_pandas_timestamp_ntz(True)

    def test_create_data_frame_to_pandas_day_time_internal(self):
        self.check_create_data_frame_to_pandas_day_time_internal(True)

    def test_toPandas_respect_session_timezone(self):
        self.check_toPandas_respect_session_timezone(True)

    def test_toPandas_with_array_type(self):
        self.check_toPandas_with_array_type(True)

    @unittest.skip("Spark Connect does not support fallback.")
    def test_toPandas_fallback_disabled(self):
        super().test_toPandas_fallback_disabled()

    @unittest.skip("Spark Connect does not support fallback.")
    def test_toPandas_fallback_enabled(self):
        super().test_toPandas_fallback_enabled()

    def test_toPandas_with_map_type(self):
        self.check_toPandas_with_map_type(True)

    def test_toPandas_with_map_type_nulls(self):
        self.check_toPandas_with_map_type_nulls(True)

    def test_createDataFrame_with_array_type(self):
        self.check_createDataFrame_with_array_type(True)

    def test_createDataFrame_with_int_col_names(self):
        self.check_createDataFrame_with_int_col_names(True)

    def test_timestamp_nat(self):
        self.check_timestamp_nat(True)

    def test_toPandas_error(self):
        self.check_toPandas_error(True)

    def test_toPandas_duplicate_field_names(self):
        self.check_toPandas_duplicate_field_names(True)

    def test_createDataFrame_duplicate_field_names(self):
        self.check_createDataFrame_duplicate_field_names(True)

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43506): Enable ArrowTests.test_toPandas_empty_columns for pandas 2.0.0.",
    )
    def test_toPandas_empty_columns(self):
        self.check_toPandas_empty_columns(True)

    def test_createDataFrame_nested_timestamp(self):
        self.check_createDataFrame_nested_timestamp(True)

    def test_toPandas_nested_timestamp(self):
        self.check_toPandas_nested_timestamp(True)

    def test_createDataFrame_udt(self):
        self.check_createDataFrame_udt(True)

    def test_toPandas_udt(self):
        self.check_toPandas_udt(True)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_parity_arrow import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
