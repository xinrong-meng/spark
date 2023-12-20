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
from abc import ABC, abstractmethod
import pstats
from threading import RLock
from typing import Dict, Optional, Tuple, cast

from pyspark.accumulators import Accumulator, AccumulatorParam, SpecialAccumulatorIds
from pyspark.profiler import CodeMapDict, MemoryProfiler, MemUsageParam, PStatsParam


ProfileResults = Dict[int, Tuple[Optional[pstats.Stats], Optional[CodeMapDict]]]


class ProfileResultsParam(AccumulatorParam[ProfileResults]):
    @staticmethod
    def zero(value: ProfileResults) -> ProfileResults:
        return value or {}

    @staticmethod
    def addInPlace(value1: ProfileResults, value2: ProfileResults) -> ProfileResults:
        if value1 is None or len(value1) == 0:
            return value2
        if value2 is None or len(value2) == 0:
            return value1

        value = value1.copy()
        for key, (perf, mem) in value2.items():
            if key in value1:
                orig_perf, orig_mem = value1[key]
            else:
                orig_perf, orig_mem = (PStatsParam.zero(None), MemUsageParam.zero(None))
            value[key] = (
                PStatsParam.addInPlace(orig_perf, perf),
                MemUsageParam.addInPlace(orig_mem, mem),
            )
        return value


class ProfilerCollector(ABC):
    def __init__(self) -> None:
        self._lock = RLock()

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

    @property
    @abstractmethod
    def _perf_profile_results(self) -> Dict[int, pstats.Stats]:
        ...

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

    @property
    @abstractmethod
    def _memory_profile_results(self) -> Dict[int, CodeMapDict]:
        ...

    @abstractmethod
    def _clear(self) -> None:
        ...


class AccumulatorProfilerCollector(ProfilerCollector):
    def __init__(self) -> None:
        super().__init__()
        self._accumulator = Accumulator(
            SpecialAccumulatorIds.SQL_UDF_PROFIER, cast(ProfileResults, {}), ProfileResultsParam()
        )

    @property
    def _perf_profile_results(self) -> Dict[int, pstats.Stats]:
        return {
            result_id: perf
            for result_id, (perf, _) in self._accumulator.value.items()
            if perf is not None
        }

    @property
    def _memory_profile_results(self) -> Dict[int, CodeMapDict]:
        return {
            result_id: mem
            for result_id, (_, mem) in self._accumulator.value.items()
            if mem is not None
        }

    def _clear(self) -> None:
        self._accumulator._value = {}
