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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.catalog.GenTable
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSelfGen
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.sql.`type`.SqlTypeName

import java.util

/**
 * Stream physical RelNode to write data into an external sink defined by a [[DynamicTableSink]].
 */
class StreamPhysicalSelfGen(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    sourceTableMapping: util.HashMap[String, String],
    sourceCatalog: util.HashMap[String, GenTable],
    sourceSql: util.HashMap[String, String],
    tableHints: util.HashMap[String, util.HashMap[String, String]])
  extends AbstractRelNode(cluster, traitSet)
  with StreamPhysicalRel {

  override def deriveRowType(): RelDataType = {
    // 创建 RelDataTypeFactory
    val typeFactory = new FlinkTypeFactory(cluster.getClass.getClassLoader)

    // 创建包含一个 String 字段的 RelDataType
    val stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR)
    val relDataType = typeFactory.builder.add("name", stringType).build
    relDataType
  }

  override def translateToExecNode(): ExecNode[_] = {

    new StreamExecSelfGen(
      unwrapTableConfig(this),
      null,
      null,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription,
      sourceTableMapping,
      sourceCatalog,
      sourceSql,
      tableHints
    )
  }

  /** Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic. */
  override def requireWatermark: Boolean = false
}
