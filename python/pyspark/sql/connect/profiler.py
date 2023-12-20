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
import pstats
from typing import Dict

from pyspark.profiler import CodeMapDict
from pyspark.sql.profiler import ProfilerCollector, ProfileResultsParam


class ConnectProfilerCollector(ProfilerCollector):
    def __init__(self) -> None:
        super().__init__()
        self._profile_results = ProfileResultsParam.zero({})

    @property
    def _perf_profile_results(self) -> Dict[int, pstats.Stats]:
        return {
            result_id: perf
            for result_id, (perf, _) in self._profile_results.items()
            if perf is not None
        }

    @property
    def _memory_profile_results(self) -> Dict[int, CodeMapDict]:
        return {
            result_id: mem
            for result_id, (_, mem) in self._profile_results.items()
            if mem is not None
        }
