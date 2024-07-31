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

package org.apache.flink.table.catalog;

import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** genTable */
public class GenTable implements Serializable {

    private Map<String, String> options;
    private List<GenTable.Column> columnList;

    private String sinkTable;
    private List<String> pks;
    private List<String> addColumnStat;

    public GenTable(
            Map<String, String> options,
            List<GenTable.Column> columnList,
            String sinkTable,
            ArrayList<String> addColumnStat,
            List<String> pks) {
        this.options = options;
        this.columnList = columnList;
        this.sinkTable = sinkTable;
        this.addColumnStat = addColumnStat;
        this.pks = pks;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public List<GenTable.Column> getColumnList() {
        return columnList;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public List<String> getPks() {
        return pks;
    }

    public List<String> getAddColumnStat() {
        return addColumnStat;
    }

    public static class Column implements Serializable {

        public Column(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }

        private String name;
        private LogicalType type;

        public String getName() {
            return name;
        }

        public LogicalType getType() {
            return type;
        }
    }
}
