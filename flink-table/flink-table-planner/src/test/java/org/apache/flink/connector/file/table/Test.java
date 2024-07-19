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

package org.apache.flink.connector.file.table;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CdcCatalog;

public class Test {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置环境配置
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.enableCheckpointing(10000);



        // 创建 JdbcCatalog
        CdcCatalog mySqlCatalog = new CdcCatalog(
                "mysql",
                "test",
                "root",
                "12345678",
                "localhost",
                "3306"
        );

        JdbcCatalog jdbcCatalog = new JdbcCatalog(
                "mysql",
                "test",
                "root",
                "12345678",
                "jdbc:mysql://localhost:3306/"
        );
        tableEnv.registerCatalog("mysql", mySqlCatalog);
        tableEnv.registerCatalog("mysql2", jdbcCatalog);

//        tableEnv.executeSql("CREATE TABLE t1 (\n"
//                + "     id INT,\n"
//                + "     name STRING,\n"
//                + "     PRIMARY KEY(id) NOT ENFORCED\n"
//                + "     ) WITH (\n"
//                + "     'connector' = 'mysql-cdc',\n"
//                + "     'hostname' = 'localhost',\n"
//                + "     'port' = '3306',\n"
//                + "     'username' = 'root',\n"
//                + "     'password' = '12345678',\n"
//                + "     'database-name' = 'test',\n"
//                + "     'table-name' = 'cdc_1')");
//        tableEnv.executeSql("CREATE TABLE t (\n" +
//                 "     id INT,\n"
//                + "     name STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'print'\n" +
//                ")");

        tableEnv.executeSql("EXECUTE STATEMENT SET  BEGIN "
                + "CREATE TABLE mysql2.test.t AS TABLE  mysql.test.cdc_1 OPTIONS('server-id' = '8001-8004');"
                + "CREATE TABLE mysql2.test.tt AS TABLE  mysql.test.cdc_1 OPTIONS('server-id' = '8005-8010');"
                + "END;");
//        tableEnv.executeSql("insert into ");



    }
}
