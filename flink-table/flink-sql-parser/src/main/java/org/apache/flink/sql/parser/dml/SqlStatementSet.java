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

package org.apache.flink.sql.parser.dml;

import org.apache.flink.sql.parser.dql.SqlCreateTableAsTable;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

/**
 * Statement Set contains a group of inserts. eg:
 *
 * <ul>
 *   execute statement set begin insert into A select * from B; insert into C select * from D; end
 * </ul>
 */
public class SqlStatementSet extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("Statement Set", SqlKind.OTHER);

    private final ArrayList<SqlNode> statements = new ArrayList<>();

    private final boolean isInsert;

    public SqlStatementSet(List<SqlNode> statements, SqlParserPos pos) {
        super(pos);
        isInsert = statements.get(0) instanceof RichSqlInsert;
        this.statements.addAll(statements);
    }

    public List<SqlNode> getStatement() {
        return statements;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return new ArrayList<>(statements);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("STATEMENT SET BEGIN");
        writer.newlineAndIndent();
        if (isInsert) {
            statements.forEach(
                    sqlNode -> {
                        RichSqlInsert insert = (RichSqlInsert) sqlNode;
                        insert.unparse(
                                writer,
                                insert.getOperator().getLeftPrec(),
                                insert.getOperator().getRightPrec());
                        writer.sep(";");
                        writer.newlineAndIndent();
                    });
        } else {
            statements.forEach(
                    sqlNode -> {
                        SqlCreateTableAsTable createTableAsTable = (SqlCreateTableAsTable) sqlNode;
                        createTableAsTable.unparse(
                                writer,
                                createTableAsTable.getOperator().getLeftPrec(),
                                createTableAsTable.getOperator().getRightPrec());
                        writer.sep(";");
                        writer.newlineAndIndent();
                    });
        }
        writer.keyword("END");
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (isInsert && !(operand instanceof RichSqlInsert)) {
            throw new UnsupportedOperationException(
                    "SqlStatementSet SqlNode only support RichSqlInsert as operand");
        } else if (!isInsert && !(operand instanceof SqlCreateTableAsTable)) {
            throw new UnsupportedOperationException(
                    "SqlStatementSet SqlNode only support SqlCreateTableAsTable as operand");
        }
        if (isInsert) {
            statements.set(i, (RichSqlInsert) operand);
        } else {
            statements.set(i, (SqlCreateTableAsTable) operand);
        }
    }
}
