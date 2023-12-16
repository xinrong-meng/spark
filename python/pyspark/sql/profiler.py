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
from functools import reduce
import pstats
from threading import RLock
from typing import Dict, List, Optional

from py4j.java_collections import JavaArray

from pyspark.profiler import MemoryProfiler, PStatsParam, has_memory_profiler

if has_memory_profiler:
    from pyspark.profiler import CodeMapDict


class ProfilerCollector:
    def __init__(self) -> None:
        self._perf_profile_results: Dict[int, Optional[pstats.Stats]] = {}
        if has_memory_profiler:
            self._memory_profile_results: Dict[int, Optional[CodeMapDict]] = {}
        self._lock = RLock()

    def _add_perf_profile_result(self, result_id: int, result: List[bytes]) -> None:
        from pyspark.worker_util import pickleSer

        with self._lock:
            self._perf_profile_results[result_id] = reduce(
                PStatsParam.addInPlace, [pickleSer.loads(r) for r in result]
            )

    def show_perf_profiles(self, id: Optional[int] = None) -> None:
        with self._lock:
            if id is not None:
                stats = self._perf_profile_results.get(id)
                if stats is not None:
                    print("=" * 60)
                    print(f"Profile of UDF<id={id}>")
                    print("=" * 60)
                    stats.sort_stats("time", "cumulative").print_stats()
            else:
                for id in sorted(self._perf_profile_results.keys()):
                    self.show_perf_profiles(id)

    if has_memory_profiler:

        def _add_memory_profile_result(self, result_id: int, result: CodeMapDict) -> None:
            with self._lock:
                self._memory_profile_results[result_id] = result

        def show_memory_profiles(self, id: Optional[int] = None) -> None:
            with self._lock:
                if id is not None:
                    code_map = self._memory_profile_results.get(id)
                    if code_map is not None:
                        print("=" * 60)
                        print(f"Profile of UDF<id={id}>")
                        print("=" * 60)
                        MemoryProfiler._show_results(code_map)
                else:
                    for id in sorted(self._memory_profile_results.keys()):
                        self.show_memory_profiles(id)

    def _clear(self) -> None:
        with self._lock:
            self._perf_profile_results.clear()
            if has_memory_profiler:
                self._memory_profile_results.clear()


class ProfileResultListener:
    def __init__(self, collector: ProfilerCollector):
        self._collector = collector

    def onUpdatePythonUDFProfiledResult(
        self, perf: Dict[int, JavaArray], mem: Dict[int, JavaArray]
    ) -> None:
        for result_id, result in perf.items():
            self._collector._add_perf_profile_result(result_id, list(result))

        # FIXME: mem

    class Java:
        implements = ["org.apache.spark.sql.execution.python.PythonUDFProfilingListener"]
