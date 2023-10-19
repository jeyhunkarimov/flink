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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.runtime.functions.table.UnnestRowsFunction
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toRowType
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexInputRef, RexNode}

import java.util
import java.util.Collections
import java.util.stream.Collectors
import scala.collection.JavaConverters._

/**
 * Planner rule that rewrites UNNEST to explode function.
 *
 * Note: This class can only be used in HepPlanner.
 */
class LogicalCorrelatePushdown(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val cor: LogicalCorrelate = call.rel(0)
    val projLeft: LogicalProject = call.rel(1)
    val funcScan: LogicalTableFunctionScan = call.rel(3)

    val newProjExprs = projLeft.getProjects.asScala.zipWithIndex.map( exprWithIdx => new RexInputRef(exprWithIdx._2, exprWithIdx._1.getType))
    val funcScanProjExprs = Seq[RexNode](newProjExprs.last).asJava
    val cluster = cor.getCluster
    val sqlFunction =
      BridgingSqlFunction.of(cluster, BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS)

    val rexCall = cluster.getRexBuilder.makeCall(
      funcScan.getRowType,
      sqlFunction,
      funcScanProjExprs
    )

    val newScan = new LogicalTableFunctionScan(
      cluster,
      cor.getTraitSet,
      Collections.emptyList(),
      rexCall,
      funcScan.getElementType,
      rexCall.getType,
      funcScan.getColumnMappings)
    val newCor = cor.copy(cor.getTraitSet, projLeft, newScan, cor.getCorrelationId, cor.getRequiredColumns, cor.getJoinType)
    val newProj: LogicalProject = new LogicalProject(cor.getCluster, cor.getTraitSet, Collections.emptyList[RelHint](), newCor, newProjExprs.asJava, cor.getRowType )
    call.transformTo(newProj)
  }
}

object LogicalCorrelatePushdown {
  val INSTANCE = new LogicalCorrelatePushdown(operand(classOf[LogicalCorrelate],
                                                      operand(classOf[LogicalProject], any),
                                                      operand(classOf[LogicalProject], operand(classOf[LogicalTableFunctionScan], any))
                                              ), "LogicalUnnestRule")
}
