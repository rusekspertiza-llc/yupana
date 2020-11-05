/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.hbase

import org.apache.hadoop.hbase.client.{ Get, Put, Scan }
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor }
import org.yupana.api.utils.ResourceUtils.using

import scala.collection.JavaConverters._

class BTreeIndexDaoHBase[K, V](
    connection: ExternalLinkHBaseConnection,
    tableName: String,
    keySerializer: K => Array[Byte],
    keyDeserializer: Array[Byte] => K,
    valueSerializer: V => Array[Byte],
    valueDeserializer: Array[Byte] => V
) {

  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val QUALIFIER: Array[Byte] = Bytes.toBytes("d")

  checkTableExistsElseCreate()

  def put(key: K, value: V): Unit = {
    using(connection.getTable(tableName)) { table =>
      val put = createPutOperation(key, value)
      table.put(put)
    }
  }

  private def createPutOperation(key: K, value: V): Put = {
    new Put(keySerializer(key)).addColumn(FAMILY, QUALIFIER, valueSerializer(value))
  }

  def batchPut(batch: Seq[(K, V)]): Unit = {
    val puts = batch.map { case (key, value) => createPutOperation(key, value) }
    using(connection.getTable(tableName)) {
      _.put(puts.asJava)
    }
  }

  def get(key: K): Option[V] = {
    using(connection.getTable(tableName)) { table =>
      val get = new Get(keySerializer(key)).addColumn(FAMILY, QUALIFIER)
      val result = table.get(get)
      Option(result.getValue(FAMILY, QUALIFIER)).map(valueDeserializer)
    }
  }

  def get(keys: Seq[K]): Map[K, V] = {
    if (keys.nonEmpty) {
      using(connection.getTable(tableName)) { table =>

        val ranges = keys.map { id =>
          val key = keySerializer(id)
          new MultiRowRangeFilter.RowRange(key, true, key, true)
        }

        val filter = new MultiRowRangeFilter(new java.util.ArrayList(ranges.asJava))
        val start = filter.getRowRanges.asScala.head.getStartRow
        val end = Bytes.padTail(filter.getRowRanges.asScala.last.getStopRow, 1)

        val scan = new Scan(start, end)
          .addFamily(FAMILY)
          .addColumn(FAMILY, QUALIFIER)
          .setFilter(filter)

        using(table.getScanner(scan)) {
          _.iterator().asScala.map { r =>
            val id = keyDeserializer(r.getRow)
            val value = valueDeserializer(r.getValue(FAMILY, QUALIFIER))
            id -> value
          }.toMap
        }
      }
    } else Map.empty
  }

  def exists(keys: Seq[K]): Map[K, Boolean] = {
    val result = get(keys)
    keys.map(k => k -> result.contains(k)).toMap
  }

  private def checkTableExistsElseCreate() {
    val descriptor = new HTableDescriptor(connection.getTableName(tableName))
      .addFamily(new HColumnDescriptor(FAMILY))
    connection.checkTablesExistsElseCreate(descriptor)
  }
}
