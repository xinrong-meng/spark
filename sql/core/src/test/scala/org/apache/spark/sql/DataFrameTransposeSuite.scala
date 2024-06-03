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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameTransposeSuite extends QueryTest with SharedSparkSession {

  test("logical plan transformation for transpose") {
    val logicalPlan = courseSales.queryExecution.logical

    // Manually construct Transpose logical plan
    val transposePlan = Transpose(
      columnsToTranspose = Seq(col("course").expr),
      columnAlias = "variable",
      orderExpression = Literal(1),
      child = logicalPlan
    )

//    println(s"Logical plan before analysis:\n$transposePlan")

    // Apply the ResolveTranspose rule
    val testAnalyzer = new Analyzer(
      new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))
    val analyzedPlan = testAnalyzer.execute(transposePlan)

//    println(s"Logical plan after analysis:\n$analyzedPlan")

    // Verify the transformed plan
    assert(analyzedPlan.isInstanceOf[Pivot])
  }
}
