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

package org.apache.flink.sql.parser.dql;

import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * {@link SqlNode} to describe the CREATE TABLE LIKE syntax. CREATE TABLE LIKE syntax is similar as
 * CREATE TABLE syntax, besides it has LIKE sub-clause to inherit property of an existed table.
 *
 * <p>Example:
 *
 * <pre>{@code
 * CREATE TABLE base_table (
 *     id BIGINT,
 *     name STRING,
 *     tstmp TIMESTAMP,
 *     PRIMARY KEY(id)
 * ) WITH (
 *     ‘connector’ = ‘kafka’,
 *     ‘connector.starting-offset’: ‘12345’,
 *     ‘format’ =  ‘json’
 * )
 *
 * CREATE TABLE derived_table (
 *      a int
 * )
 * WITH (
 *   'connector' = 'jdbc',
 *   'url' = 'http://localhost:10000',
 *   'table-name' = 'derivedTable'
 * )
 * LIKE base_table;
 * }</pre>
 */
public class SqlCreateTableAsTable extends SqlCreateTable {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE AS", SqlKind.CREATE_TABLE);

    private final SqlIdentifier sourceTableName;
    private final SqlNodeList sourcePropertyList;

    public SqlCreateTableAsTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlNodeList partitionKeyList,
            @Nullable SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            SqlIdentifier sourceTableName,
            SqlNodeList sourcePropertyList,
            boolean isTemporary,
            boolean ifNotExists) {
        super(
                OPERATOR,
                pos,
                tableName,
                columnList,
                tableConstraints,
                propertyList,
                partitionKeyList,
                watermark,
                comment,
                isTemporary,
                ifNotExists);
        this.sourceTableName = sourceTableName;
        this.sourcePropertyList = sourcePropertyList;
    }

    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlIdentifier getSourceTableName() {
        return sourceTableName;
    }

    public SqlNodeList getSourcePropertyList() {
        return sourcePropertyList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(sourceTableName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.keyword("TABLE");
        this.sourceTableName.unparse(writer, leftPrec, rightPrec);

        if (this.sourcePropertyList.size() > 0) {
            writer.keyword("OPTIONS");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : sourcePropertyList) {
                SqlUnparseUtils.printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }
}
