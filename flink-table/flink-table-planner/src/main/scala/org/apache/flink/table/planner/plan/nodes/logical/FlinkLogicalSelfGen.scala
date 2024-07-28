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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.catalog.GenTable
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.calcite.Sink

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.sql.`type`.SqlTypeName

import java.util

/**
 * Sub-class of [[Sink]] that is a relational expression which writes out data of input node into a
 * [[DynamicTableSink]].
 */
class FlinkLogicalSelfGen(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    sourceTableMapping: util.HashMap[String, String],
    sourceCatalog: util.HashMap[String, GenTable],
    sourceSql: util.HashMap[String, String],
    tableHints: util.HashMap[String, util.HashMap[String, String]])
  extends AbstractRelNode(cluster, traitSet)
  with FlinkLogicalRel {

  def getSourceTableMapping: util.HashMap[String, String] = sourceTableMapping
  def getSourceCatalog: util.HashMap[String, GenTable] = sourceCatalog
  def getSourceSql: util.HashMap[String, String] = sourceSql
  def getTableHints: util.HashMap[String, util.HashMap[String, String]] = tableHints

  override def deriveRowType(): RelDataType = {
    // 创建 RelDataTypeFactory
    val typeFactory = new FlinkTypeFactory(cluster.getClass.getClassLoader)

    // 创建包含一个 String 字段的 RelDataType
    val stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR)
    val relDataType = typeFactory.builder.add("name", stringType).build
    relDataType
  }
}
