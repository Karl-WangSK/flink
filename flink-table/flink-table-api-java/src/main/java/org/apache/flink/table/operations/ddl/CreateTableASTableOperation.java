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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.operations.*;

import java.util.HashMap;

/** Operation to describe a CREATE TABLE AS statement. */
@Internal
public class CreateTableASTableOperation implements ModifyOperation {

    private final HashMap<String, String> sourceTableMapping;

    private final HashMap<String, String> sourceSql;

    private final HashMap<String, GenTable> sourceCatalog;
    private final HashMap<String, HashMap<String, String>> tableHints;

    public CreateTableASTableOperation(
            HashMap<String, String> sourceTableMapping,
            HashMap<String, GenTable> sourceCatalog,
            HashMap<String, String> sourceSql,
            HashMap<String, HashMap<String, String>> tableHints) {
        this.sourceTableMapping = sourceTableMapping;
        this.sourceCatalog = sourceCatalog;
        this.sourceSql = sourceSql;
        this.tableHints = tableHints;
    }

    public HashMap<String, String> getSourceTableMapping() {
        return sourceTableMapping;
    }

    public HashMap<String, String> getSourceSql() {
        return sourceSql;
    }

    public HashMap<String, GenTable> getSourceCatalog() {
        return sourceCatalog;
    }

    public HashMap<String, HashMap<String, String>> getTableHints() {
        return tableHints;
    }

    @Override
    public String asSummaryString() {
        return tableHints.toString();
    }

    @Override
    public QueryOperation getChild() {
        return null;
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
