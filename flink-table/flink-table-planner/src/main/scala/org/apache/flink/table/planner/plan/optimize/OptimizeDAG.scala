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
package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.calcite.{FlinkRelBuilder, RexFactory}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchInterval, MiniBatchIntervalTrait, MiniBatchIntervalTraitDef, MiniBatchMode, ModifyKindSet, ModifyKindSetTraitDef, UpdateKind, UpdateKindTraitDef}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.{LegacySink, Sink}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalDataStreamScan, StreamPhysicalIntermediateTableScan, StreamPhysicalLegacyTableSourceScan, StreamPhysicalRel, StreamPhysicalTableSourceScan}
import org.apache.flink.table.planner.plan.optimize.program.{FlinkStreamProgram, StreamOptimizeContext}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

class OptimizeDAG(planner: StreamPlanner) extends StreamCommonSubGraphBasedOptimizer(planner) {

  private val optimm = new Optimm

  private def propagateUpdateKindAndMiniBatchInterval(
      block: RelNodeBlock,
      updateBeforeRequired: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): Unit = {
    val blockLogicalPlan = block.getPlan
    val optPlan = block.getOptimizedPlan
    // infer updateKind and miniBatchInterval with required trait
    val optimizedPlan = optimm.optimize(optPlan, updateBeforeRequired, planner.catalogManager, None)
//    val optimizedPlan = optPlan

    // propagate the inferred updateKind and miniBatchInterval to the child blocks
    propagateTraits(optimizedPlan)

    block.children.foreach {
      child =>
        propagateUpdateKindAndMiniBatchInterval(
          child,
          updateBeforeRequired = child.isUpdateBeforeRequired,
          miniBatchInterval = child.getMiniBatchInterval,
          isSinkBlock = false)
    }

    def propagateTraits(rel: RelNode): Unit =
      rel match {
        case _: StreamPhysicalDataStreamScan | _: StreamPhysicalIntermediateTableScan |
            _: StreamPhysicalLegacyTableSourceScan | _: StreamPhysicalTableSourceScan =>
          val scan = rel.asInstanceOf[TableScan]
          val updateKindTrait = scan.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
          val miniBatchIntervalTrait = scan.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
          val tableName = scan.getTable.getQualifiedName.mkString(".")
          val inputBlocks = block.children.filter(b => tableName.equals(b.getOutputTableName))
          Preconditions.checkArgument(inputBlocks.size <= 1)
          if (inputBlocks.size == 1) {
            val childBlock = inputBlocks.head
            // propagate miniBatchInterval trait to child block
            childBlock.setMiniBatchInterval(miniBatchIntervalTrait.getMiniBatchInterval)
            // propagate updateKind trait to child block
            val requireUB = updateKindTrait.updateKind == UpdateKind.BEFORE_AND_AFTER
            childBlock.setUpdateBeforeRequired(requireUB || childBlock.isUpdateBeforeRequired)
          }
        case ser: StreamPhysicalRel => ser.getInputs.foreach(e => propagateTraits(e))
        case _ => // do nothing
      }

  }

  private def optimizeBlock(block: RelNodeBlock, isSinkBlock: Boolean, idx: Int = 0): Unit = {
    block.children.zipWithIndex.foreach {
      zippedChild =>
        if (zippedChild._1.getNewOutputNode.isEmpty) {
          optimizeBlock(zippedChild._1, isSinkBlock = false, zippedChild._2)
        }
    }

    def prope(rel: RelNode): RelNode =
      rel match {
        case sc: StreamPhysicalIntermediateTableScan =>
          val name = sc.intermediateTable.getNames.get(0)
          val targetBlock = block.children.filter(b => b.getOutputTableName == name)

          val pl = targetBlock(0).getOptimizedPlan
          val intermediateRelTable = createIntermediateRelTable(
            name,
            pl,
            pl.getTraitSet
              .getTrait(ModifyKindSetTraitDef.INSTANCE)
              .modifyKindSet,
            block.isUpdateBeforeRequired
          )

//          sc.intermediateTable = intermediateRelTable
          val newSc = new StreamPhysicalIntermediateTableScan(
            sc.getCluster,
            sc.getTraitSet,
            intermediateRelTable,
            sc.getRowType)
//          parent.replaceInput(pos, newSc)
//          val modifyKindSetTrait =
//            pl.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
////          sc.intermediateTable.relNode = pl
//          sc.intermediateTable.modifyKindSet = modifyKindSetTrait.modifyKindSet

          newSc
//          val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
//          block.children(idx).setNewOutputNode(newTableScan)
        case ser: StreamPhysicalRel => {
          val children = ser.getInputs.map(e => prope(e))
          ser.copy(ser.getTraitSet, children)
        }
        case _ => throw new UnsupportedOperationException("ASDASDAS")
      }

    val newOp = prope(block.getOptimizedPlan)
    block.setOptimizedPlan(newOp)
    val blockLogicalPlan = block.getPlan
    val downstreamPlan =
      if (block.children.nonEmpty)
        Some(block.children(idx).getOptimizedPlan.asInstanceOf[StreamPhysicalRel])
      else None

    blockLogicalPlan match {
      case _: LegacySink | _: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimm.optimize(
          block.getOptimizedPlan,
          block.isUpdateBeforeRequired,
          planner.catalogManager,
          downstreamPlan)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimm.optimize(
          block.getOptimizedPlan,
          block.isUpdateBeforeRequired,
          planner.catalogManager,
          downstreamPlan)
        val modifyKindSetTrait = optimizedPlan.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val name =
          if (block.getOutputTableName == null) createUniqueIntermediateRelTableName
          else block.getOutputTableName
        val intermediateRelTable = createIntermediateRelTable(
          name,
          optimizedPlan,
          modifyKindSetTrait.modifyKindSet,
          block.isUpdateBeforeRequired)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setOptimizedPlan(optimizedPlan)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
    }

  }

  def optimizeDAG(sb: Seq[RelNodeBlock]): Seq[RelNodeBlock] = {
    val sinkBlocks = sb

    sinkBlocks.foreach(resetIntermediateResult)
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    sinkBlocks.foreach {
      b =>
        propagateUpdateKindAndMiniBatchInterval(
          b,
          b.isUpdateBeforeRequired,
          b.getMiniBatchInterval,
          isSinkBlock = true)
    }
    // clear the intermediate result
    sinkBlocks.foreach(resetIntermediateResult)

    // optimize recursively RelNodeBlock
    sinkBlocks.foreach(
      b => {
        optimizeBlock(b, isSinkBlock = true)
      })

    sinkBlocks.foreach(s => s.setNewOutputNode(s.getOptimizedPlan))
//    val ch = ChangelogPlanUtils.getChangelogMode(
//      sinkBlocks(0).getOptimizedPlan.asInstanceOf[StreamPhysicalRel])
    sinkBlocks
  }

  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
//    block.setOutputTableName(null)
//    block.setOptimizedPlan(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

}
