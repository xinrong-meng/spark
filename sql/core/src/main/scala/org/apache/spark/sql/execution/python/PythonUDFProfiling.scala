/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.python

import java.util.{Collections, Map => JMap, WeakHashMap => JWeakHashMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{ObservedMetrics, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{ExprId, GenericRowWithSchema, PythonUDF}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.CollectionAccumulator

object PythonUDFProfiling {
  type ProfilerAccumulator = CollectionAccumulator[Array[Byte]]

  val PROFILER_RESULT_KEY_PREFIX = "__python_udf_profiler__"

  private[PythonUDFProfiling] val PROFILER_RESULT_SCHEMA = StructType(Seq(
    StructField("perf", ArrayType(BinaryType), nullable = false),
    StructField("mem", ArrayType(BinaryType), nullable = false)
  ))

  private[PythonUDFProfiling]
  val ProfilerAccumulators: mutable.Map[ExprId, (ProfilerAccumulator, ProfilerAccumulator)] =
    Collections.synchronizedMap(
      new JWeakHashMap[ExprId, (ProfilerAccumulator, ProfilerAccumulator)]).asScala
}

private[sql] trait PythonUDFProfiling { self: SparkPlan =>
  import PythonUDFProfiling._

  def profiler: Option[String] = SQLConf.get.pythonUDFProfiler

  def udfs: Seq[PythonUDF]

  private lazy val _profilerAccumulators: Map[Long, (ProfilerAccumulator, ProfilerAccumulator)] = {
    udfs.map { udf =>
      udf.resultId.id -> ProfilerAccumulators.getOrElseUpdate(udf.resultId, {
        val perfAccum = self.sparkContext.collectionAccumulator[Array[Byte]]
        val memAccum = self.sparkContext.collectionAccumulator[Array[Byte]]
        (perfAccum, memAccum)
      })
    }.toMap
  }

  def profilerAccumulators: Map[Long, ProfilerAccumulator] = {
    profiler match {
      case Some("perf") => _profilerAccumulators.view.mapValues(_._1).toMap
      case Some("memory") => _profilerAccumulators.view.mapValues(_._2).toMap
      case _ => Map.empty
    }
  }

  def collectedProfilerResults: Map[String, Row] = {
    _profilerAccumulators.collect {
      case (id, (perf, mem)) if !perf.isZero || !mem.isZero =>
        s"$PROFILER_RESULT_KEY_PREFIX$id" ->
          new GenericRowWithSchema(
            Array(perf.value.asScala.toArray, mem.value.asScala.toArray), PROFILER_RESULT_SCHEMA)
    }
  }
}

trait PythonUDFProfilingListener {
  def onUpdatePythonUDFProfiledResult(
    perf: JMap[Long, Array[Array[Byte]]], mem: JMap[Long, Array[Array[Byte]]]): Unit
}

class PythonUDFProfilingListenerHelper(listener: PythonUDFProfilingListener)
    extends QueryExecutionListener {
  import PythonUDFProfiling._

  private def onUpdatePythonUDFProfiledResult(qe: QueryExecution): Unit = {
    val results = qe.observedMetrics.collect {
      case (key, value) if key.startsWith(PROFILER_RESULT_KEY_PREFIX) =>
        key.drop(PROFILER_RESULT_KEY_PREFIX.length).toLong ->
          (value.getAs[Array[Array[Byte]]](0), value.getAs[Array[Array[Byte]]](1))
    }

    if (results.nonEmpty) {
      val perf = results.map { case (id, (perf, _)) => id -> perf }
      val mem = results.map { case (id, (_, mem)) => id -> mem }

      listener.onUpdatePythonUDFProfiledResult(perf.asJava, mem.asJava)
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    onUpdatePythonUDFProfiledResult(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    onUpdatePythonUDFProfiledResult(qe)
  }
}

class PythonProfilingObservation(resultId: Long, session: SparkSession) extends ObservedMetrics {
  import PythonUDFProfiling._

  session.listenerManager.register(listener)

  override val name: String = s"$PROFILER_RESULT_KEY_PREFIX${resultId}"

  @volatile private var _metrics: Option[Map[String, Any]] = None

  override protected[sql] def metrics: Option[Map[String, Any]] = _metrics

  override protected[spark] def onFinish(qe: QueryExecution): Unit = {
    synchronized {
      val row = qe.observedMetrics.get(name)
      this._metrics = row.map(r => r.getValuesMap[Any](r.schema.fieldNames.toImmutableArraySeq))
      if (metrics.isDefined) {
        notifyAll()
        session.listenerManager.unregister(listener)
      }
    }
  }
}
