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

import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableConfigUtils, TableTestBase, TestPartitionableSourceFactory}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

import scala.collection.JavaConversions._

/** Test for [[LogicalCorrelatePushdownRule]]. */
class LogicalCorrelatePushdownRuleTest
  extends TableTestBase {
  protected val util: BatchTableTestUtil = batchTestUtil()

  @throws(classOf[Exception])
  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(
          RuleSets.ofList(
            CoreRules.PROJECT_CORRELATE_TRANSPOSE,
            LogicalCorrelatePushdownRule.INSTANCE
          ))
        .build()
    )

    val tableDdl =
      "CREATE TABLE MyTable (\n" +
        "  id int,\n" +
        "  name string,\n" +
        "  other_names ARRAY<STRING>,\n" +
        "  age int,\n" +
        "  checks ARRAY<BOOLEAN>,\n" +
        "  zip_codes ARRAY<INT>,\n" +
        "  nationality string)\n" +
        "  WITH (\n" +
        " 'bounded' = 'true',\n" +
        " 'connector' = 'values'\n" +
    ")";

    util.tableEnv.executeSql(tableDdl)
    print(123)
  }

  @Test
  def testSingleNestedFieldPushdown(): Unit = {
//    util.verifyRelPlan("SELECT nationality FROM MyTable, UNNEST(zip_codes) AS t1")
//    util.verifyRelPlan("SELECT id FROM MyTable, UNNEST(checks) AS t1")
    util.verifyRelPlan("SELECT name FROM MyTable, UNNEST(other_names) AS t1")
  }

//  @Test
//  def testMultipleNestedFieldsPushdown(): Unit = {
//    util.verifyRelPlan("SELECT nationality FROM MyTable, UNNEST(zip_codes) AS t1, UNNEST(checks) AS t2, UNNEST(other_names) AS t3")
//    util.verifyRelPlan("SELECT nationality FROM MyTable, UNNEST(other_names) AS t1, UNNEST(zip_codes) AS t2, UNNEST(checks) AS t3")
//    util.verifyRelPlan("SELECT nationality FROM MyTable, UNNEST(checks) AS t1, UNNEST(zip_codes) AS t2, UNNEST(checks) AS t3")
//    util.verifyRelPlan("SELECT nationality, name FROM MyTable, UNNEST(checks) AS t1, UNNEST(zip_codes) AS t2, UNNEST(checks) AS t3")
//    util.verifyRelPlan("SELECT id, nationality FROM MyTable, UNNEST(checks) AS t1, UNNEST(zip_codes) AS t2, UNNEST(checks) AS t3")
//  }

//  @Test
//  def testShouldNotPushdownField(): Unit = {
//    util.verifyRelPlan("SELECT * FROM MyTable, UNNEST(zip_codes) AS t1, UNNEST(checks) AS t2, UNNEST(other_names) AS t3")
//  }
}
