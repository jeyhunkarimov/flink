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
import org.apache.flink.table.planner.plan.optimize.program.{FlinkChainedProgram, FlinkStreamProgram, FlinkStreamProgram2, FlinkStreamProgram3, StreamOptimizeContext}
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

/** A [[CommonSubGraphBasedOptimizer]] for Stream. */
class StreamCommonSubGraphBasedOptimizer(planner: StreamPlanner)
  extends CommonSubGraphBasedOptimizer {

  private def isCalciteConfigEmpty: Boolean = {
    val tableConfig = planner.getTableConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig)
    calciteConfig.getStreamProgram.isEmpty
  }
  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    val tableConfig = planner.getTableConfig
    // build RelNodeBlock plan
    val sinkBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, tableConfig)
    // infer trait properties for sink block
    sinkBlocks.foreach {
      sinkBlock =>
        // don't require update before by default
        sinkBlock.setUpdateBeforeRequired(false)

        val miniBatchInterval: MiniBatchInterval =
          if (tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            val miniBatchLatency =
              tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY).toMillis
            Preconditions.checkArgument(
              miniBatchLatency > 0,
              "MiniBatch Latency must be greater than 0 ms.",
              null)
            new MiniBatchInterval(miniBatchLatency, MiniBatchMode.ProcTime)
          } else {
            MiniBatchIntervalTrait.NONE.getMiniBatchInterval
          }
        sinkBlock.setMiniBatchInterval(miniBatchInterval)
    }

    if (sinkBlocks.size == 1) {
      // If there is only one sink block, the given relational expressions are a simple tree
      // (only one root), not a dag. So many operations (e.g. infer and propagate
      // requireUpdateBefore) can be omitted to save optimization time.
      val block = sinkBlocks.head
      val optimizedTree = optimizeTree(
        block.getPlan,
        block.isUpdateBeforeRequired,
        block.getMiniBatchInterval,
        isSinkBlock = true,
        programs = getOptimizePrograms)
      block.setOptimizedPlan(optimizedTree)

      if (isCalciteConfigEmpty) {
        val dd = new OptimizeDAG(planner)
        val newBl = dd.optimizeDAG(sinkBlocks)
        val optimizedTree2 = optimizeTree2(
          newBl.head.getOptimizedPlan,
          newBl.head.isUpdateBeforeRequired,
          newBl.head.getMiniBatchInterval,
          isSinkBlock = true)
        newBl.head.setOptimizedPlan(optimizedTree2)

        val optimizedTree3 = optimizeTree3(
          newBl.head.getOptimizedPlan,
          newBl.head.isUpdateBeforeRequired,
          newBl.head.getMiniBatchInterval,
          isSinkBlock = true)
        newBl.head.setOptimizedPlan(optimizedTree3)

        return newBl
      } else {
        return sinkBlocks
      }

      //      //    newBl.foreach(resetIntermediateResult)
//      //    // optimize recursively RelNodeBlock
//      //    newBl.foreach(b => optimizeBlock(b, isSinkBlock = true))
//
    }

    // TODO FLINK-24048: Move changeLog inference out of optimizing phase
    // infer modifyKind property for each blocks independently
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))

    val dd = new OptimizeDAG(planner)
    val newBl = dd.optimizeDAG(sinkBlocks)
//    newBl.foreach(resetIntermediateResult)
//    // optimize recursively RelNodeBlock
    newBl.foreach(b => optimizeBlock2(b, isSinkBlock = true))
    newBl.foreach(b => optimizeBlock3(b, isSinkBlock = true))
    newBl

    //    val sd = ChangelogPlanUtils.getChangelogMode(newBl(2).children(0).getOptimizedPlan.asInstanceOf[StreamPhysicalRel])
//    val ssa = newBl(0).getOptimizedPlan

    // infer and propagate updateKind and miniBatchInterval property for each blocks
//    sinkBlocks.foreach {
//      b =>
//        propagateUpdateKindAndMiniBatchInterval(
//          b,
//          b.isUpdateBeforeRequired,
//          b.getMiniBatchInterval,
//          isSinkBlock = true)
//    }
//    // clear the intermediate result
//     sinkBlocks.foreach(resetIntermediateResult)
//    // optimize recursively RelNodeBlock
//    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
//    sinkBlocks
  }

  private def optimizeBlock3(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
        //        if (child.getNewOutputNode.isEmpty) {
        optimizeBlock3(child, isSinkBlock = false)
        //        }
    }

    val blockLogicalPlan = block.getOptimizedPlan
    blockLogicalPlan match {
      case _: LegacySink | _: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree3(
          blockLogicalPlan,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree3(
          o,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = isSinkBlock)
        val modifyKindSetTrait = optimizedPlan.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(
          name,
          optimizedPlan,
          modifyKindSetTrait.modifyKindSet,
          block.isUpdateBeforeRequired)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  private def optimizeBlock2(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
//        if (child.getNewOutputNode.isEmpty) {
        optimizeBlock2(child, isSinkBlock = false)
//        }
    }

    val blockLogicalPlan = block.getOptimizedPlan
    blockLogicalPlan match {
      case _: LegacySink | _: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree2(
          blockLogicalPlan,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree2(
          o,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = isSinkBlock)
        val modifyKindSetTrait = optimizedPlan.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(
          name,
          optimizedPlan,
          modifyKindSetTrait.modifyKindSet,
          block.isUpdateBeforeRequired)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  private def optimizeBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child, isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case _: LegacySink | _: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree(
          blockLogicalPlan,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = true,
          programs = getOptimizePrograms
        )
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree(
          o,
          updateBeforeRequired = block.isUpdateBeforeRequired,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = isSinkBlock,
          programs = getOptimizePrograms)
        val modifyKindSetTrait = optimizedPlan.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(
          name,
          optimizedPlan,
          modifyKindSetTrait.modifyKindSet,
          block.isUpdateBeforeRequired)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  private def getOptimizePrograms: FlinkChainedProgram[StreamOptimizeContext] = {
    val tableConfig = planner.getTableConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig)
    val programs = calciteConfig.getStreamProgram
      .getOrElse(FlinkStreamProgram.buildProgram(tableConfig))
    programs
  }

  /**
   * Generates the optimized [[RelNode]] tree from the original relational node tree.
   *
   * @param relNode
   *   The root node of the relational expression tree.
   * @param updateBeforeRequired
   *   True if UPDATE_BEFORE message is required for updates
   * @param miniBatchInterval
   *   mini-batch interval of the block.
   * @param isSinkBlock
   *   True if the given block is sink block.
   * @return
   *   The optimized [[RelNode]] tree
   */
  private def optimizeTree(
      relNode: RelNode,
      updateBeforeRequired: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean,
      programs: FlinkChainedProgram[StreamOptimizeContext]): RelNode = {

    val tableConfig = planner.getTableConfig
    val context = unwrapContext(relNode)

    programs.optimize(
      relNode,
      new StreamOptimizeContext() {

        override def isBatchMode: Boolean = false

        override def getTableConfig: TableConfig = tableConfig

        override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

        override def getCatalogManager: CatalogManager = planner.catalogManager

        override def getModuleManager: ModuleManager = planner.moduleManager

        override def getRexFactory: RexFactory = context.getRexFactory

        override def getFlinkRelBuilder: FlinkRelBuilder = planner.createRelBuilder

        override def isUpdateBeforeRequired: Boolean = updateBeforeRequired

        def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

        override def needFinalTimeIndicatorConversion: Boolean = isSinkBlock

        override def getClassLoader: ClassLoader = context.getClassLoader
      }
    )
  }

  private def optimizeTree2(
      relNode: RelNode,
      updateBeforeRequired: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): RelNode = {
    val tableConfig = planner.getTableConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig)
    // //  // // // // // // //
    val programs = calciteConfig.getStreamProgram
      .getOrElse(FlinkStreamProgram2.buildProgram(tableConfig))

    Preconditions.checkNotNull(programs)

    val context = unwrapContext(relNode)

    programs.optimize(
      relNode,
      new StreamOptimizeContext() {

        override def isBatchMode: Boolean = false

        override def getTableConfig: TableConfig = tableConfig

        override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

        override def getCatalogManager: CatalogManager = planner.catalogManager

        override def getModuleManager: ModuleManager = planner.moduleManager

        override def getRexFactory: RexFactory = context.getRexFactory

        override def getFlinkRelBuilder: FlinkRelBuilder = planner.createRelBuilder

        override def isUpdateBeforeRequired: Boolean = updateBeforeRequired

        def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

        override def needFinalTimeIndicatorConversion: Boolean = isSinkBlock

        override def getClassLoader: ClassLoader = context.getClassLoader
      }
    )
  }

  private def optimizeTree3(
      relNode: RelNode,
      updateBeforeRequired: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): RelNode = {

    val tableConfig = planner.getTableConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig)
    val programs = calciteConfig.getStreamProgram
      .getOrElse(FlinkStreamProgram3.buildProgram(tableConfig))

    Preconditions.checkNotNull(programs)

    val context = unwrapContext(relNode)

    programs.optimize(
      relNode,
      new StreamOptimizeContext() {

        override def isBatchMode: Boolean = false

        override def getTableConfig: TableConfig = tableConfig

        override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

        override def getCatalogManager: CatalogManager = planner.catalogManager

        override def getModuleManager: ModuleManager = planner.moduleManager

        override def getRexFactory: RexFactory = context.getRexFactory

        override def getFlinkRelBuilder: FlinkRelBuilder = planner.createRelBuilder

        override def isUpdateBeforeRequired: Boolean = updateBeforeRequired

        def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

        override def needFinalTimeIndicatorConversion: Boolean = isSinkBlock

        override def getClassLoader: ClassLoader = context.getClassLoader
      }
    )
  }

  protected def createIntermediateRelTable(
      name: String,
      relNode: RelNode,
      modifyKindSet: ModifyKindSet,
      isUpdateBeforeRequired: Boolean): IntermediateRelTable = {
    val uniqueKeys = getUniqueKeys(relNode)
    val fmq = FlinkRelMetadataQuery
      .reuseOrCreate(planner.createRelBuilder.getCluster.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(relNode)
    val windowProperties = fmq.getRelWindowProperties(relNode)
    val statistic = FlinkStatistic
      .builder()
      .uniqueKeys(uniqueKeys)
      .relModifiedMonotonicity(monotonicity)
      .relWindowProperties(windowProperties)
      .build()
    new IntermediateRelTable(
      Collections.singletonList(name),
      relNode,
      modifyKindSet,
      isUpdateBeforeRequired,
      fmq.getUpsertKeys(relNode),
      statistic)
  }

  private def getUniqueKeys(relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq =
      FlinkRelMetadataQuery.reuseOrCreate(planner.createRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.filter(_.nonEmpty).map {
        uniqueKey =>
          val keys = new util.HashSet[String]()
          uniqueKey.asList().foreach(idx => keys.add(rowType.getFieldNames.get(idx)))
          keys
      }
    } else {
      null
    }
  }

  override protected def postOptimize(expanded: Seq[RelNode]): Seq[RelNode] = {
    StreamNonDeterministicPhysicalPlanResolver.resolvePhysicalPlan(expanded, planner.getTableConfig)
  }
}
