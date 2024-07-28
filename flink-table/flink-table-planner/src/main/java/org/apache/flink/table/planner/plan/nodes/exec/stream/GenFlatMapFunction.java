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
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.catalog.GenTable;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GenFlatMapFunction implements FlatMapFunction<HashMap, Row>, Serializable {
    protected final Logger logger = LoggerFactory.getLogger(GenFlatMapFunction.class);

    private ArrayList<String> columnNameList = new ArrayList<>();
    private ArrayList<LogicalType> columnTypeList = new ArrayList<>();
    private String tableName;
    private ZoneId sinkTimeZone = ZoneId.of("UTC");

    protected List<ConvertType> typeConverterList =
            Lists.newArrayList(
                    this::convertVarCharType,
                    this::convertDateType,
                    this::convertVarBinaryType,
                    this::convertBigIntType,
                    this::convertFloatType,
                    this::convertDecimalType,
                    this::convertTimestampType);

    @FunctionalInterface
    public interface ConvertType extends Serializable {
        Optional<Object> convert(Object target, LogicalType logicalType);
    }

    public GenFlatMapFunction(HashMap<String, GenTable> sourceCatalog, String tableName) {
        buildColumn(columnNameList, columnTypeList, sourceCatalog.get(tableName).getColumnList());
        this.tableName = tableName;
    }

    protected void buildColumn(
            List<String> columnNameList,
            List<LogicalType> columnTypeList,
            List<GenTable.Column> columns) {
        for (GenTable.Column column : columns) {
            columnNameList.add(column.getName());
            columnTypeList.add(column.getType());
        }
    }

    @Override
    public void flatMap(HashMap value, Collector<Row> out) throws Exception {
        try {
            switch (value.get("op").toString()) {
                case "r":
                case "c":
                    rowCollect(
                            columnNameList,
                            columnTypeList,
                            out,
                            RowKind.INSERT,
                            (Map) value.get("after"));
                    break;
                case "d":
                    rowCollect(
                            columnNameList,
                            columnTypeList,
                            out,
                            RowKind.DELETE,
                            (Map) value.get("before"));
                    break;
                case "u":
                    rowCollect(
                            columnNameList,
                            columnTypeList,
                            out,
                            RowKind.UPDATE_BEFORE,
                            (Map) value.get("before"));
                    rowCollect(
                            columnNameList,
                            columnTypeList,
                            out,
                            RowKind.UPDATE_AFTER,
                            (Map) value.get("after"));
                    break;
                default:
            }
        } catch (Exception e) {
            logger.error("SchemaTable: {}  - Exception {}", tableName, e.toString());
            throw e;
        }
    }

    private void rowCollect(
            List<String> columnNameList,
            List<LogicalType> columnTypeList,
            Collector<Row> out,
            RowKind rowKind,
            Map value) {
        Row row = Row.withPositions(rowKind, columnNameList.size());
        for (int i = 0; i < columnNameList.size(); i++) {
            row.setField(i, convertValue(value.get(columnNameList.get(i)), columnTypeList.get(i)));
        }
        out.collect(row);
    }

    protected Object convertValue(Object value, LogicalType logicalType) {
        if (value == null) {
            return null;
        }

        for (ConvertType convertType : typeConverterList) {
            Optional<Object> result = convertType.convert(value, logicalType);
            if (result.isPresent()) {
                return result.get();
            }
        }
        return value;
    }

    protected Optional<Object> convertVarBinaryType(Object value, LogicalType logicalType) {
        if (logicalType instanceof VarBinaryType) {
            // VARBINARY AND BINARY is converted to String with encoding base64 in FlinkCDC.
            if (value instanceof String) {
                return Optional.of(DatatypeConverter.parseBase64Binary(value.toString()));
            }

            return Optional.of(value);
        }
        return Optional.empty();
    }

    protected Optional<Object> convertBigIntType(Object value, LogicalType logicalType) {
        if (logicalType instanceof BigIntType) {
            if (value instanceof Integer) {
                return Optional.of(((Integer) value).longValue());
            }

            return Optional.of(value);
        }
        return Optional.empty();
    }

    protected Optional<Object> convertFloatType(Object value, LogicalType logicalType) {
        if (logicalType instanceof FloatType) {
            if (value instanceof Float) {
                return Optional.of(value);
            }

            if (value instanceof Double) {
                return Optional.of(((Double) value).floatValue());
            }

            return Optional.of(Float.parseFloat(value.toString()));
        }
        return Optional.empty();
    }

    protected Optional<Object> convertDecimalType(Object value, LogicalType logicalType) {
        if (logicalType instanceof DecimalType) {
            final DecimalType decimalType = (DecimalType) logicalType;
            return Optional.ofNullable(
                    DecimalData.fromBigDecimal(
                            new BigDecimal((String) value),
                            decimalType.getPrecision(),
                            decimalType.getScale()));
        }
        return Optional.empty();
    }

    protected Optional<Object> convertTimestampType(Object value, LogicalType logicalType) {
        if (logicalType instanceof TimestampType) {
            if (value instanceof Integer) {
                return Optional.of(
                        Instant.ofEpochMilli(((Integer) value).longValue())
                                .atZone(sinkTimeZone)
                                .toLocalDateTime());
            } else if (value instanceof String) {
                return Optional.of(
                        Instant.parse((String) value).atZone(sinkTimeZone).toLocalDateTime());
            } else {
                TimestampType logicalType1 = (TimestampType) logicalType;
                if (logicalType1.getPrecision() == 3) {
                    return Optional.of(
                            Instant.ofEpochMilli((long) value)
                                    .atZone(sinkTimeZone)
                                    .toLocalDateTime());
                } else if (logicalType1.getPrecision() > 3) {
                    return Optional.of(
                            Instant.ofEpochMilli(
                                            ((long) value)
                                                    / (long)
                                                            Math.pow(
                                                                    10,
                                                                    logicalType1.getPrecision()
                                                                            - 3))
                                    .atZone(sinkTimeZone)
                                    .toLocalDateTime());
                }
                return Optional.of(
                        Instant.ofEpochSecond(((long) value))
                                .atZone(sinkTimeZone)
                                .toLocalDateTime());
            }
        }
        return Optional.empty();
    }

    protected Optional<Object> convertDateType(Object target, LogicalType logicalType) {
        if (logicalType instanceof DateType) {
            return Optional.of(
                    StringData.fromString(
                            Instant.ofEpochMilli((long) target)
                                    .atZone(ZoneId.systemDefault())
                                    .toLocalDate()
                                    .toString()));
        }
        return Optional.empty();
    }

    protected Optional<Object> convertVarCharType(Object target, LogicalType logicalType) {
        if (logicalType instanceof VarCharType) {
            return Optional.of(((String) target));
        }
        return Optional.empty();
    }
}
