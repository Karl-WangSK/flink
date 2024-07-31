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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置环境配置
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.enableCheckpointing(1000);

        tableEnv.executeSql(
                "CREATE CATALOG mysql\n"
                        + "WITH (\n"
                        + "  'type' = 'jdbc',\n"
                        + "  'default-database' = 'sfe_flow_db',\n"
                        + "  'username' = 'xx',\n"
                        + "  'password' = 'xx',\n"
                        + "  'ip' = 'xx',\n"
                        + "  'port' = '3310'\n"
                        + ")");

        tableEnv.executeSql(
                "CREATE CATALOG holo\n"
                        + "WITH (\n"
                        + "  'type' = 'jdbc',\n"
                        + "  'default-database' = 'rsodw_dev',\n"
                        + "  'username' = 'xx',\n"
                        + "  'password' = 'xx',\n"
                        + "  'base-url' = 'jdbc:postgresql://xxx:/'\n"
                        + ")");

        tableEnv.executeSql(
                "EXECUTE STATEMENT SET  BEGIN "
                        + "CREATE TABLE holo.rsodw_dev.flink.sfe_flow_db_ent_standard_day_sale_copy AS TABLE  mysql.sfe_flow_db.ent_standard_day_sale OPTIONS('server-id' = '8001-8004','parallelism.default' = '4')\n" +
                    " ADD COLUMNS (cast(now()  as varchar) as t) ;"
                        + "END;");

    }
}
