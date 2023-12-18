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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StreamStatementSet stmtSet = tableEnv.createStatementSet();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b STRING\n"
                        + ") with (\n"
                        + " 'connector' = 'datagen', "
                        + " 'number-of-rows' = '10' )";

        String srcTableDdl2 =
                "CREATE TABLE sink1 (\n"
                        + "  a bigint\n"
                        + ") with (\n"
                        + " 'connector' = 'print' )";

        String srcTableDdl3 =
                "CREATE TABLE sink2 (\n"
                        + "  b STRING\n"
                        + ") with (\n"
                        + " 'connector' = 'print' )";
        tableEnv.executeSql(srcTableDdl);
        tableEnv.executeSql(srcTableDdl2);
        tableEnv.executeSql(srcTableDdl3);

        Table table1 = tableEnv.sqlQuery("SELECT a  FROM MyTable where a > 10");
        Table table2 = tableEnv.sqlQuery("SELECT b  FROM MyTable");

        stmtSet.addInsert("sink1", table1);
        stmtSet.addInsert("sink2", table2);

        stmtSet.execute().wait();

        //        tableEnv.executeSql("insert into sink1 select * from " + table1 + ";" + "insert
        // into sink2 select * from " + table2);
        //        tableEnv.executeSql("insert into sink2 select * from " + table2);

        //        Table table3 = tableEnv.sqlQuery("SELECT *  FROM sink1 union select * from sink2"
        // );
        //
        //        tableEnv.toDataStream(table3).print();
        //        String queryNotPushing = "  select a from MyTable";
        //        String r2 = tableEnv.explainSql(queryNotPushing);
        //        System.out.println("NOT Pushing plan:");
        //        System.out.println(r2);
    }
}
