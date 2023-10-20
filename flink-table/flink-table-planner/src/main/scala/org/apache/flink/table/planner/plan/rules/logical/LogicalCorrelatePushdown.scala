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
class LogicalCorrelatePushdown(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description) {


  override def matches(call: RelOptRuleCall): Boolean = {
    LogicalCorrelatePushdown.TT += 1
    if (LogicalCorrelatePushdown.TT ==  1 || LogicalCorrelatePushdown.TT == 3) {
      return true
    } else {
      return false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val cor: LogicalCorrelate = call.rel(0)
    val projLeft: LogicalProject = call.rel(1)
    //    val tableScan: LogicalTableScan = call.rel(2)
    val projRight: LogicalProject = call.rel(2)
    val funcScan: LogicalTableFunctionScan = call.rel(3)

    val isTableScan = CorrelateUtil.getRel(projLeft.getInput(0)).isInstanceOf[LogicalTableScan]

    def getInputRefs(logicalProject: LogicalProject) : Seq[RexInputRef] = logicalProject.getProjects.asScala.filter(expr => expr.isInstanceOf[RexInputRef]).asInstanceOf[Seq[RexInputRef]]

    def deriveFuncScanIndexes(rexNode: RexNode) : Option[Seq[Int]] = {
      rexNode match {
        case rx: RexCall =>
          val operands = rx.getOperands
          val res = operands.asScala.filter(op => op.isInstanceOf[RexFieldAccess]).map {
            op => op.asInstanceOf[RexFieldAccess].getField.getIndex
          }
          Some(res)
        case _ => None
      }
    }

    val funcScanFieldIndexes = deriveFuncScanIndexes(funcScan.getCall).getOrElse(return)
    val leafProf = findLeafProjectOp(cor).getOrElse(return)
    val leafProjExprs = getInputRefs(leafProf)
    val leftProjExprs = getInputRefs(projLeft)

    val projMap = if (isTableScan) {
      Map(leafProjExprs.zipWithIndex.map {exprWithIdx =>
        (exprWithIdx._1.getIndex, new RexInputRef(exprWithIdx._2, exprWithIdx._1.getType))
      } : _*)
    }
    else {
      Map(funcScanFieldIndexes.zipWithIndex.map { exprWithIdx => {
        val idx =  leftProjExprs.size - exprWithIdx._2 - 1
        (exprWithIdx._1, new RexInputRef(idx, leftProjExprs(idx).getType))
      }
      }: _*)
    }
    val newFuncScanProjExprs: Seq[RexNode] = funcScanFieldIndexes.map(idx => projMap(idx))

    val cluster = cor.getCluster
    val sqlFunction =
      BridgingSqlFunction.of(cluster, BuiltInFunctionDefinitions.INTERNAL_UNNEST_ROWS)

    val rexCall = cluster.getRexBuilder.makeCall(
      funcScan.getRowType,
      sqlFunction,
      newFuncScanProjExprs.asJava
    )
    //    val ds = funcScan.getCall.asInstanceOf[RexCall].getOperands.get(0).asInstanceOf[RexFieldAccess].getField.getIndex
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
    //    val projIndexes = leftProjExprs.zipWithIndex.map(exprWithIndex => new RexInputRef(exprWithIndex._2, exprWithIndex._1.getType).asInstanceOf[RexNode]).asJava
    //    val newProjTop: LogicalProject = LogicalProject.create(newCor, projLeft.getHints, projIndexes, cor.getRowType)

    call.transformTo(newCor)
  }

  private def findLeafProjectOp(root: RelNode) : Option[LogicalProject] = {
    root match {
      case vertex: HepRelVertex => findLeafProjectOp(vertex.getCurrentRel)
      case project: LogicalProject if CorrelateUtil.getRel(project.getInput(0)).isInstanceOf[LogicalTableScan] =>
        Some(project)
      case _ => if (root.getInputs.isEmpty) {
        None
      } else {
        findLeafProjectOp(root.getInput(0))
      }
    }
  }
}

object LogicalCorrelatePushdown {
  val INSTANCE = new LogicalCorrelatePushdown(operand(classOf[LogicalCorrelate],
    operand(classOf[LogicalProject], any),
    operand(classOf[LogicalProject], operand(classOf[LogicalTableFunctionScan], any))
  ), "LogicalCorrelatePushdown")
  var TT = 0
}
