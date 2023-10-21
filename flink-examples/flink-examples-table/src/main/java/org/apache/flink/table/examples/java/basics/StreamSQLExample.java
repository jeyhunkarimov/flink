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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Simple example for demonstrating the use of SQL on a table backed by a {@link DataStream} in Java
 * DataStream API.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>convert two bounded data streams to tables,
 *   <li>register a table as a view under a name,
 *   <li>run a stream SQL query on registered and unregistered tables,
 *   <li>and convert the table back to a data stream.
 * </ul>
 *
 * <p>The example executes a single Flink job. The results are written to stdout.
 */
public final class StreamSQLExample {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());


        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c ARRAY<STRING>\n"
//                        + "  d ARRAY<INT>,\n"
//                        + "  e int\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'csv',"
                        + " 'path' = 'D:\\tmp2')";
        tableEnv.executeSql(srcTableDdl);

//        String ins = "insert into MyTable values (1, 2, ARRAY ['ceyhun', 'sevinc']), (10, 11, ARRAY ['laura', 'lalos'])";
//        tableEnv.executeSql(ins);

        String queryNotPushing = "SELECT b FROM MyTable, UNNEST(c) AS t2";
        String r2 = tableEnv.explainSql(queryNotPushing);
        System.out.println("NOT Pushing plan:");
        System.out.println(r2);

//        String queryPushing = "SELECT a FROM MyTable";
//        String r1 = tableEnv.explainSql(queryPushing);
//        System.out.println("Pushing plan:");
//        System.out.println(r1);

//        final Table result = tableEnv.sqlQuery(queryNotPushing);
//        result.execute().print();
        // convert the Table back to an insert-only DataStream of type `Order`
//
//        // after the table program is converted to a DataStream program,
//        // we must use `env.execute()` to submit the job

















//        // set up the Java DataStream API
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // set up the Java Table API
//        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        final DataStream<Order> orderA =
//                env.fromCollection(
//                        Arrays.asList(
//                                new Order(1L, "beer", 3, new Long[]{1L,2L,3L}),
//                                new Order(1L, "diaper", 4, new Long[]{1L,2L,3L}),
//                                new Order(3L, "rubber", 2, new Long[]{1L,2L,3L})));
//
//        final DataStream<Order> orderB =
//                env.fromCollection(
//                        Arrays.asList(
//                                new Order(2L, "pen", 3, new Long[]{1L,2L,3L}),
//                                new Order(2L, "rubber", 3, new Long[]{1L,2L,3L}),
//                                new Order(4L, "beer", 1, new Long[]{1L,2L,3L})));
//
//        // convert the first DataStream to a Table object
//        // it will be used "inline" and is not registered in a catalog
//        final Table tableA = tableEnv.fromDataStream(orderA);
//
//        // convert the second DataStream and register it as a view
//        // it will be accessible under a name
//        tableEnv.createTemporaryView("TableB", orderB);
//
//        String queryNoPush = "SELECT product, tag FROM "
//                + tableA
//                + " CROSS JOIN UNNEST(grades) AS tag"
//                + " WHERE amount < 4 ";
//
//        String queryShouldPush = "SELECT product FROM "
//                + tableA
//                + " WHERE amount < 4 ";
//
////        String r1 = tableEnv.explainSql(queryNoPush);
////        System.out.println(r1);
//
//        String r2 = tableEnv.explainSql(queryShouldPush);
//        System.out.println(r2);
//
//        // union the two tables
//        final Table result = tableEnv.sqlQuery(queryNoPush);
//        // convert the Table back to an insert-only DataStream of type `Order`
//        tableEnv.toDataStream(result).print();
//
//        // after the table program is converted to a DataStream program,
//        // we must use `env.execute()` to submit the job
//        env.execute();
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /** Simple POJO. */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Long[] grades;
        // for POJO detection in DataStream API
        public Order() {}

        // for structured type detection in Table API
        public Order(Long user, String product, int amount, Long[] grades) {
            this.user = user;
            this.product = product;
            this.amount = amount;
            this.grades = grades;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + ", grades="
                    + grades
                    + '}';
        }
    }
}
