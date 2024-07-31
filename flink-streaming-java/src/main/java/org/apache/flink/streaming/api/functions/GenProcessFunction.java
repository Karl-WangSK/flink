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
package org.apache.flink.streaming.api.functions;

import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class GenProcessFunction extends ProcessFunction<HashMap, HashMap> implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(GenProcessFunction.class);

    private HashMap<String, OutputTag<HashMap<String, Object>>> tagMap;

    public GenProcessFunction(HashMap<String, String> sourceTableMapping) {
        HashMap<String, OutputTag<HashMap<String, Object>>> tagMap = new HashMap<>();
        for (String key : sourceTableMapping.keySet()) {
            String[] split = key.split("\\.");
            tagMap.put(split[1] + "." + split[2], new OutputTag<HashMap<String, Object>>(key) {});
        }
        this.tagMap = tagMap;
    }

    @Override
    public void processElement(
            HashMap map, ProcessFunction<HashMap, HashMap>.Context ctx, Collector<HashMap> out)
            throws Exception {
        LinkedHashMap<String, Object> source = (LinkedHashMap<String, Object>) map.get("source");
        String tableName = createTableName(source);
        OutputTag<HashMap<String, Object>> outputTag = tagMap.get(tableName);

        ctx.output(outputTag, map);
    }

    protected String createTableName(LinkedHashMap<String, Object> source) {
        return source.get("db").toString() + "." + source.get("table").toString();
    }
}
