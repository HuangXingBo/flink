/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLocalRef, RexNode, RexProgram}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.FunctionCodeGenerator
import org.apache.flink.table.functions.python.PythonFunctionInfo
import org.apache.flink.table.plan.nodes.CommonPythonCalc
import org.apache.flink.table.plan.nodes.datastream.DataStreamPythonCalc.PYTHON_SCALAR_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.CRowProcessRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions

import scala.collection.JavaConversions._

/**
  * RelNode for Python ScalarFunctions.
  */
class DataStreamPythonCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    inputSchema: RowSchema,
    schema: RowSchema,
    calcProgram: RexProgram,
    ruleDescription: String)
  extends DataStreamCalcBase(
    cluster,
    traitSet,
    input,
    inputSchema,
    schema,
    calcProgram,
    ruleDescription)
  with CommonPythonCalc {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new DataStreamPythonCalc(
      cluster,
      traitSet,
      child,
      inputSchema,
      schema,
      program,
      ruleDescription)
  }

  private lazy val pythonRexCalls = calcProgram.getProjectList
    .map(calcProgram.expandLocalRef)
    .filter(_.isInstanceOf[RexCall])
    .map(_.asInstanceOf[RexCall])
    .toArray

  private lazy val forwardedFields: Array[Int] = calcProgram.getProjectList
    .map(calcProgram.expandLocalRef)
    .filter(_.isInstanceOf[RexInputRef])
    .map(_.asInstanceOf[RexInputRef].getIndex)
    .toArray

  private lazy val (pythonUdfInputOffsets, pythonFunctionInfos) =
    extractPythonScalarFunctionInfos(pythonRexCalls)

  private lazy val resultProjectList: Array[RexNode] = {
    var idx = 0
    calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .map {
        case pythonCall: RexCall =>
          val inputRef = new RexInputRef(forwardedFields.length + idx, pythonCall.getType)
          idx += 1
          inputRef
        case node => node
      }
      .toArray
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {
    val config = planner.getConfig

    val inputDataStream =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)

    val inputParallelism = inputDataStream.getParallelism

    val pythonOperatorResultTypeInfo = new RowTypeInfo(
      forwardedFields.map(inputSchema.fieldTypeInfos.get(_)) ++
        pythonRexCalls.map(node => FlinkTypeFactory.toTypeInfo(node.getType)): _*)

    // Constructs the Python operator
    val pythonOperatorInputType = TypeConversions.fromLegacyInfoToDataType(
      inputSchema.typeInfo)
    val pythonOperatorOutputType = TypeConversions.fromLegacyInfoToDataType(
      pythonOperatorResultTypeInfo)
    val pythonOperator = getPythonScalarFunctionOperator(
      pythonOperatorInputType, pythonOperatorOutputType, pythonUdfInputOffsets)

    val pythonDataStream = inputDataStream
      .transform(
        calcOpName(calcProgram, getExpressionString),
        CRowTypeInfo(pythonOperatorResultTypeInfo),
        pythonOperator)
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)

    val generator = new FunctionCodeGenerator(
      config, false, pythonOperatorResultTypeInfo)

    val genFunction = generateFunction(
      generator,
      ruleDescription,
      schema,
      resultProjectList,
      None,
      config,
      classOf[ProcessFunction[CRow, CRow]])

    val processFunc = new CRowProcessRunner(
      genFunction.name,
      genFunction.code,
      CRowTypeInfo(schema.typeInfo))

    pythonDataStream
      .process(processFunc)
      .name(calcOpName(calcProgram, getExpressionString))
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)
  }

  private[flink] def getPythonScalarFunctionOperator(
      inputType: DataType,
      outputType: DataType,
      udfInputOffsets: Array[Int]) = {
    val clazz = Class.forName(PYTHON_SCALAR_FUNCTION_OPERATOR_NAME)
    val ctor = clazz.getConstructor(
      classOf[Array[PythonFunctionInfo]],
      classOf[DataType],
      classOf[DataType],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      pythonFunctionInfos,
      inputType,
      outputType,
      udfInputOffsets,
      forwardedFields)
      .asInstanceOf[OneInputStreamOperator[CRow, CRow]]
  }
}

object DataStreamPythonCalc {
  val PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.PythonScalarFunctionOperator"
}
