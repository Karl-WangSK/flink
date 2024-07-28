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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.GenTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.*;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stream {@link ExecNode} to write data into an external sink defined by a {@link
 * DynamicTableSink}.
 */
@ExecNodeMetadata(
        name = "stream-exec-selfgen",
        version = 1,
        consumedOptions = {
            "table.exec.sink.not-null-enforcer",
            "table.exec.sink.type-length-enforcer",
            "table.exec.sink.upsert-materialize",
            "table.exec.sink.keyed-shuffle",
            "table.exec.sink.rowtime-inserter"
        },
        producedTransformations = {
            CommonExecSink.CONSTRAINT_VALIDATOR_TRANSFORMATION,
            CommonExecSink.PARTITIONER_TRANSFORMATION,
            CommonExecSink.UPSERT_MATERIALIZE_TRANSFORMATION,
            CommonExecSink.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecSink.SINK_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecSelfGen extends CommonExecSink
        implements StreamExecNode<Object>, Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(StreamExecSelfGen.class);

    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";

    /** New introduced state metadata to enable operator-level state TTL configuration. */
    public static final String STATE_NAME = "sinkMaterializeState";

    private final HashMap<String, String> sourceTableMapping;
    private final HashMap<String, String> sourceSql;

    private final HashMap<String, GenTable> sourceCatalog;
    private final HashMap<String, HashMap<String, String>> tableHints;

    private final List<String> dbs = new ArrayList<>();

    private final List<String> tables = new ArrayList<>();

    protected List<ModifyOperation> modifyOperations = new ArrayList<>();

    public StreamExecSelfGen(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            InputProperty inputProperty,
            LogicalType outputType,
            String description,
            HashMap<String, String> sourceTableMapping,
            HashMap<String, GenTable> sourceCatalog,
            HashMap<String, String> sourceSql,
            HashMap<String, HashMap<String, String>> tableHints) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecSelfGen.class),
                ExecNodeContext.newPersistedConfig(StreamExecSelfGen.class, tableConfig),
                tableSinkSpec,
                inputChangelogMode,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                sourceTableMapping,
                sourceCatalog,
                sourceSql,
                tableHints);
    }

    @JsonCreator
    public StreamExecSelfGen(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK) DynamicTableSinkSpec tableSinkSpec,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            HashMap<String, String> sourceTableMapping,
            HashMap<String, GenTable> sourceCatalog,
            HashMap<String, String> sourceSql,
            HashMap<String, HashMap<String, String>> tableHints) {
        super(
                id,
                context,
                persistedConfig,
                tableSinkSpec,
                inputChangelogMode,
                false, // isBounded
                inputProperties,
                outputType,
                description);
        this.sourceTableMapping = sourceTableMapping;
        this.sourceCatalog = sourceCatalog;
        this.sourceSql = sourceSql;
        this.tableHints = tableHints;

        for (String tableName : sourceCatalog.keySet()) {
            String[] tableCatalog = tableName.split("\\.");
            dbs.add(tableCatalog[1]);
            tables.add(tableCatalog[1] + "." + tableCatalog[2]);
        }
    }

    public List<Transformation<Row>> translateToPlanInternal(PlannerBase planner) {
        ObjectMapper objectMapper = new ObjectMapper();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<GenTable> values = new ArrayList<>(sourceCatalog.values());
        Map<String, String> sourceConfig = values.get(0).getOptions();

        MySqlSourceBuilder<String> sourceBuilder =
                MySqlSource.<String>builder()
                        .hostname(sourceConfig.get("hostname"))
                        .port(Integer.parseInt(sourceConfig.get("port")))
                        .databaseList(String.join(",", dbs))
                        .tableList(String.join(",", tables))
                        .username(sourceConfig.get("username"))
                        .password(sourceConfig.get("password"))
                        .serverId(sourceConfig.get("server-id"))
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial());

        DataStreamSource<String> mySQLCdcSource =
                env.fromSource(
                        sourceBuilder.build(),
                        WatermarkStrategy.noWatermarks(),
                        "MySQL CDC Source");

        HashMap<String, OutputTag<HashMap>> tagMap = new HashMap<>();
        for (String key : sourceTableMapping.keySet()) {
            tagMap.put(key, new OutputTag<HashMap>(key) {});
        }
        SingleOutputStreamOperator<HashMap> mapOperator =
                mySQLCdcSource
                        .map(x -> objectMapper.readValue(x, HashMap.class))
                        .returns(HashMap.class);

        partitionByTableAndPrimarykey(mapOperator);
        SingleOutputStreamOperator<HashMap> processDs =
                mapOperator.process(new GenProcessFunction(sourceTableMapping));

        List<Transformation<Row>> transformations =
                tagMap.keySet().stream()
                        .map(
                                tableName -> {
                                    OutputTag<HashMap> tag = tagMap.get(tableName);
                                    DataStream<HashMap> filterOperator = shunt(processDs, tag);
                                    logger.info("Build {} shunt successful...", tableName);
                                    ArrayList<String> columnNameList = new ArrayList<>();
                                    ArrayList<LogicalType> columnTypeList = new ArrayList<>();
                                    buildColumn(
                                            columnNameList,
                                            columnTypeList,
                                            sourceCatalog.get(tableName).getColumnList());
                                    DataStream<Row> rowDataDataStream =
                                            buildRow(
                                                            filterOperator,
                                                            columnNameList,
                                                            columnTypeList,
                                                            tableName)
                                                    .forward();
                                    rowDataDataStream.getTransformation().setName(tableName);
                                    logger.info("Build {} flatMap successful...", tableName);
                                    logger.info("Start build {} sink...", tableName);
                                    return rowDataDataStream.getTransformation();
                                })
                        .collect(Collectors.toList());

        return transformations;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {

        return null;
    }

    protected void partitionByTableAndPrimarykey(SingleOutputStreamOperator<HashMap> mapOperator) {
        HashMap<String, GenTable> dbTableMap = new HashMap<>();
        for (String key : sourceCatalog.keySet()) {
            String[] split = key.split("\\.");
            dbTableMap.put(split[1] + "." + split[2], sourceCatalog.get(key));
        }
        mapOperator.partitionCustom(
                new Partitioner<String>() {
                    @Override
                    public int partition(String key, int numPartitions) {
                        return Math.abs(key.hashCode()) % numPartitions;
                    }
                },
                map -> {
                    LinkedHashMap source = (LinkedHashMap) map.get("source");
                    String tableName =
                            source.get("db").toString() + "." + source.get("table").toString();
                    GenTable table = dbTableMap.get(tableName);
                    List<String> primaryKeys = table.getPks();
                    Map value;
                    ArrayList<String> valueList = new ArrayList<>();
                    switch (map.get("op").toString()) {
                        case "d":
                            value = (Map) map.get("before");
                            break;
                        default:
                            value = (Map) map.get("after");
                    }
                    for (String primaryKey : primaryKeys) {
                        valueList.add(value.get(primaryKey).toString());
                    }
                    return tableName + String.join("_", valueList);
                });
        mapOperator.name("PartitionByPrimarykey");
    }

    protected DataStream<HashMap> shunt(
            SingleOutputStreamOperator<HashMap> processOperator, OutputTag<HashMap> tag) {
        processOperator.forward();
        return processOperator.getSideOutput(tag).forward();
    }

    protected DataStream<Row> buildRow(
            DataStream<HashMap> filterOperator,
            ArrayList<String> columnNameList,
            ArrayList<LogicalType> columnTypeList,
            String schemaTableName) {
        TypeInformation<?>[] typeInformation =
                TypeConversions.fromDataTypeToLegacyInfo(
                        TypeConversions.fromLogicalToDataType(
                                columnTypeList.toArray(new LogicalType[0])));

        return filterOperator.flatMap(
                new GenFlatMapFunction(sourceCatalog, schemaTableName),
                new RowTypeInfo(typeInformation, columnNameList.toArray(new String[0])));
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
    protected Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey) {
        return null;
    }
}
