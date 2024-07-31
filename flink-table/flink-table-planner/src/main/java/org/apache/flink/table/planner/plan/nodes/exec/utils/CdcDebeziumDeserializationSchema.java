package org.apache.flink.table.planner.plan.nodes.exec.utils;

import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CdcDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws JsonProcessingException {
        Struct value = (Struct) sourceRecord.value();
        Struct source = value.getStruct("source");
        HashMap<String, String> sourceMap = new HashMap<>();
        HashMap<String, Object> afterMap = new HashMap<>();
        HashMap<String, Object> beforeMap = new HashMap<>();
        sourceMap.put("db", source.getString("db"));
        sourceMap.put("table", source.getString("table"));

        HashMap<String, Object> result = new HashMap<>();


        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        Struct after = value.getStruct("after");
        Struct before = value.getStruct("before");
        switch (operation){
            case READ:
            case CREATE:
                for (Field field : after.schema().fields()) {
                    afterMap.put(field.name(), after.getWithoutDefault(field.name()));
                }
                result.put("op", "c");
                break;
            case DELETE:
                for (Field field : before.schema().fields()) {
                    beforeMap.put(field.name(), before.getWithoutDefault(field.name()));
                }
                result.put("op", "d");
                break;
            case UPDATE:
                for (Field field : before.schema().fields()) {
                    beforeMap.put(field.name(), before.getWithoutDefault(field.name()));
                }
                for (Field field : after.schema().fields()) {
                    afterMap.put(field.name(), after.getWithoutDefault(field.name()));
                }
                result.put("op", "u");
                break;
            default:
                throw new RuntimeException("不存在改操作");
        }
        result.put("after", afterMap);
        result.put("before", beforeMap);
        result.put("source", sourceMap);

        String  valueAsString = objectMapper.writeValueAsString(result);
        collector.collect(valueAsString);
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
