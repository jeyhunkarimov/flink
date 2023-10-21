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
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexCall, RexFieldAccess, RexInputRef, RexNode}
import org.apache.flink.table.planner.plan.utils.CorrelateUtil

import java.util
import java.util.Collections
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Planner rule that rewrites UNNEST to explode function.
 *
 * Note: This class can only be used in HepPlanner.
 */
class LogicalCorrelatePushdownRule(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description) {


  override def matches(call: RelOptRuleCall): Boolean = {
    val projLeft: LogicalProject = call.rel(1)
    val funcScan: LogicalTableFunctionScan = call.rel(3)

    val refExprs = deriveRequiredFields(funcScan.getCall, projLeft).getOrElse(return false)
    val targetExprs = projLeft.getRowType.getFieldList.asScala

    val matchAll = refExprs.map(expr => {

      val refIdx = expr.getIndex
      val refName = expr.getName
      if (refIdx < targetExprs.size && targetExprs(refIdx).getName.eq(refName)) false else true
    }).forall(b => b)

    matchAll
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val cor: LogicalCorrelate = call.rel(0)
    val projLeft: LogicalProject = call.rel(1)
    val projRight: LogicalProject = call.rel(2)
    val funcScan: LogicalTableFunctionScan = call.rel(3)

    def getInputRefs(logicalProject: LogicalProject) : Seq[RexInputRef] = logicalProject.getProjects.asScala.filter(expr => expr.isInstanceOf[RexInputRef]).asInstanceOf[Seq[RexInputRef]]

    val requiredFields = deriveRequiredFields(funcScan.getCall, projLeft).getOrElse(return)
    val targetExprIndexes = requiredFields.map(field => projLeft.getRowType.getFieldList.asScala.indexWhere(pfield => pfield.getType.eq(field.getType)))
    val targetExprFlatten = getInputRefs(projLeft).zipWithIndex.map(exprWithIdx => new RexInputRef(exprWithIdx._2, exprWithIdx._1.getType))
    val newFuncScanProjExprs: Seq[RexNode] = targetExprIndexes.map(idx => targetExprFlatten(idx))

    val cluster = cor.getCluster
    val sqlFunction =
      BridgingSqlFunction.of(cluster, BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS)

    val rexCall = cluster.getRexBuilder.makeCall(
      funcScan.getRowType,
      sqlFunction,
      newFuncScanProjExprs.asJava
    )

    val newScan = new LogicalTableFunctionScan(
      cluster,
      cor.getTraitSet,
      Collections.emptyList(),
      rexCall,
      funcScan.getElementType,
      rexCall.getType,
      funcScan.getColumnMappings)

    val newProjRight: LogicalProject = projRight.copy(projRight.getTraitSet, newScan, projRight.getProjects, projRight.getRowType)
    val newCor = cor.copy(cor.getTraitSet, projLeft, newProjRight, cor.getCorrelationId, cor.getRequiredColumns, cor.getJoinType)

    call.transformTo(newCor)
  }

  private def deriveRequiredFields(rexNode: RexNode, projLeft: LogicalProject): Option[Seq[RelDataTypeField]] = {
    rexNode match {
      case rx: RexCall =>
        val operands = rx.getOperands
        val res = operands.asScala.map {
          case exprAccess: RexFieldAccess  => Some(exprAccess.getField)
          case exprRef: RexInputRef  => {
            val fieldIndex = exprRef.getIndex
            val projExprs = projLeft.getRowType.getFieldList
            if (fieldIndex < projExprs.size()) Some(projExprs.get(fieldIndex)) else None
          }
          case _ => None
        }
        val withValues = res.forall(_.isDefined)
        if (withValues) Some(res.map(r => r.get)) else None
      case _ => None
    }
  }
}

object LogicalCorrelatePushdownRule {
  val INSTANCE = new LogicalCorrelatePushdownRule(operand(classOf[LogicalCorrelate],
    operand(classOf[LogicalProject], any),
    operand(classOf[LogicalProject], operand(classOf[LogicalTableFunctionScan], any))
  ), "LogicalCorrelatePushdown")
}
