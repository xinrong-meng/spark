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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, Cast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project, Sort, Transpose}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.types.{AtomicType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String


class ResolveTranspose(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def leastCommonType(dataTypes: Seq[DataType]): DataType = {
    if (dataTypes.isEmpty) {
      StringType
    } else {
      dataTypes.reduce { (dt1, dt2) =>
        TypeCoercion.findTightestCommonType(dt1, dt2).getOrElse {
          throw new IllegalArgumentException(s"No common type found for $dt1 and $dt2")
        }
      }
    }
  }

  private def transposeMatrix(
      fullCollectedRows: Array[InternalRow],
      nonIndexColumnNames: Seq[String],
      nonIndexColumnDataTypes: Seq[DataType]): Array[Array[Any]] = {
    val numTransposedRows = fullCollectedRows.head.numFields - 1
    val numTransposedCols = fullCollectedRows.length + 1
    val finalMatrix = Array.ofDim[Any](numTransposedRows, numTransposedCols)

    // Example of the original DataFrame:
    // +---+-----+-----+
    // | id|col1 |col2 |
    // +---+-----+-----+
    // |  1|  10 |  20 |
    // |  2|  30 |  40 |
    // +---+-----+-----+
    //
    // After transposition, the finalMatrix will look like:
    // [
    //   ["col1", 10, 30],  // Transposed row for col1
    //   ["col2", 20, 40]   // Transposed row for col2
    // ]

    for (i <- 0 until numTransposedRows) {
      // Insert non-index column name as the first element in each transposed row
      finalMatrix(i)(0) = UTF8String.fromString(nonIndexColumnNames(i))

      for (j <- 1 until numTransposedCols) {
        // Insert the transposed data

        // Example: If j = 2, then row = fullCollectedRows(1)
        // This corresponds to the second row of the original DataFrame: InternalRow(2, 30, 40)
        val row = fullCollectedRows(j - 1)

        // Example: If i = 0 (for "col1"), and j = 2,
        // then finalMatrix(0)(2) corresponds to row.get(1, nonIndexColumnDataTypes(0)),
        // which accesses the value 30 from InternalRow(2, 30, 40)
        finalMatrix(i)(j) = row.get(i + 1, nonIndexColumnDataTypes(i))
      }
    }
    finalMatrix
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(TreePattern.UNRESOLVED_TRANSPOSE)) {
    case t @ UnresolvedTranspose(indexColumn, child) if indexColumn.resolved && child.resolved =>

      // Cast the index column to StringType
      val indexColumnAsString = indexColumn match {
        case attr: Attribute if attr.dataType.isInstanceOf[AtomicType] =>
          Alias(Cast(attr, StringType), attr.name)()
        case attr: Attribute =>
          throw new IllegalArgumentException(
            s"Index column must be of atomic type, but found: ${attr.dataType}")
        case _ =>
          throw new IllegalArgumentException(
            s"Index column must be an atomic attribute")
      }

      // Cast non-index columns to the least common type
      val nonIndexColumnsAttr = child.output.filterNot(
        _.exprId == indexColumn.asInstanceOf[Attribute].exprId)
      val nonIndexTypes = nonIndexColumnsAttr.map(_.dataType)
      val commonType = leastCommonType(nonIndexTypes)
      val nonIndexColumnsAsLCT = nonIndexColumnsAttr.map { attr =>
        Alias(Cast(attr, commonType), attr.name)()
      }

      // Sort by index column values, and collect the casted frame
      val allCastCols = indexColumnAsString +: nonIndexColumnsAsLCT
      val sortedChild = Sort(
        Seq(SortOrder(indexColumn.asInstanceOf[Attribute], Ascending)),
        global = true,
        child
      )
      val projectAllCastCols = Project(allCastCols, sortedChild)
      val queryExecution = sparkSession.sessionState.executePlan(projectAllCastCols)
      val fullCollectedRows = queryExecution.executedPlan.executeCollect()

      if (fullCollectedRows.isEmpty) {
        // Return a DataFrame with a single column "key" containing non-index column names
        val keyAttr = AttributeReference("key", StringType, nullable = false)()
        val keyValues = nonIndexColumnsAttr.map(
          _.name).map(name => UTF8String.fromString(name))
        val keyRows = keyValues.map(value => InternalRow(value))

        LocalRelation(Seq(keyAttr), keyRows)
      } else {
        val maxValues = sparkSession.sessionState.conf.dataFrameTransposeMaxValues
        if (fullCollectedRows.length > maxValues) {
          throw new IllegalArgumentException(
            s"Transposing the DataFrame exceeds the maximum allowed number of " +
            s"values: $maxValues. Actual number of values: ${fullCollectedRows.length}."
          )
        }

        // Transpose the matrix
        val nonIndexColumnNames = nonIndexColumnsAttr.map(_.name)
        val nonIndexColumnDataTypes = projectAllCastCols.output.tail.map(attr => attr.dataType)
        val transposedMatrix = transposeMatrix(
          fullCollectedRows, nonIndexColumnNames, nonIndexColumnDataTypes)
        val transposedInternalRows = transposedMatrix.map { row =>
          InternalRow.fromSeq(row.toIndexedSeq)
        }

        // Construct output attributes
        val keyAttr = AttributeReference("key", StringType, nullable = false)()
        val transposedColumnNames = fullCollectedRows.map { row => row.getString(0) }
        val valueAttrs = transposedColumnNames.map { value =>
          AttributeReference(
            value,
            commonType
          )()
        }

        val hasNonIndexColumns = nonIndexColumnsAttr.nonEmpty
        val transposeOutput = (keyAttr +: valueAttrs).toIndexedSeq
        val transposeData = transposedInternalRows.toIndexedSeq
        Transpose(transposeOutput, transposeData, hasNonIndexColumns)
      }
  }
}
