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

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{ FilterList, SingleColumnValueFilter }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ CompareOperator, TableExistsException, TableName }
import org.yupana.api.query.Query
import org.yupana.api.utils.GroupByIterator
import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.QueryStates.QueryState
import org.yupana.core.model.TsdbQueryMetrics._
import org.yupana.core.model.{ MetricData, QueryStates, TsdbQueryMetrics }
import org.yupana.core.utils.metric.{ MetricCollector, NoMetricCollector }
import org.yupana.hbase.TsdbQueryMetricsDaoHBase._

import java.time.{ Duration, Instant, OffsetDateTime, ZoneOffset }
import scala.jdk.CollectionConverters._
import scala.util.Using

object TsdbQueryMetricsDaoHBase {
  val TABLE_NAME: String = "ts_query_metrics"
  val ID_FAMILY: Array[Byte] = Bytes.toBytes("idf")
  val FAMILY: Array[Byte] = Bytes.toBytes("f")
  val QUERY_QUALIFIER: Array[Byte] = Bytes.toBytes(queryColumn)
  val START_DATE_QUALIFIER: Array[Byte] = Bytes.toBytes(startDateColumn)
  val TOTAL_DURATION_QUALIFIER: Array[Byte] = Bytes.toBytes(totalDurationColumn)
  val STATE_QUALIFIER: Array[Byte] = Bytes.toBytes(stateColumn)
  val ENGINE_QUALIFIER: Array[Byte] = Bytes.toBytes(engineColumn)
  val RUNNING_PARTITIONS_QUALIFIER: Array[Byte] = Bytes.toBytes("runningPartitions")
  val BATCH_SIZE = 10000
  val DEFAULT_LIMIT = 1000

  def getTableName(namespace: String): TableName = TableName.valueOf(namespace, TABLE_NAME)
}

class TsdbQueryMetricsDaoHBase(connection: Connection, namespace: String)
    extends TsdbQueryMetricsDao
    with StrictLogging {

  override def saveQueryMetrics(
      query: Query,
      partitionId: Option[String],
      startDate: Long,
      queryState: QueryState,
      totalDuration: Long,
      metricValues: Map[String, MetricData],
      sparkQuery: Boolean
  ): Unit = withTables {

    val key = rowKey(query.id, partitionId)
    val engine = if (sparkQuery) "SPARK" else "STANDALONE"

    val put = new Put(key)
    put.addColumn(FAMILY, QUERY_QUALIFIER, Bytes.toBytes(query.toString))
    put.addColumn(FAMILY, TOTAL_DURATION_QUALIFIER, Bytes.toBytes(totalDuration))
    put.addColumn(FAMILY, STATE_QUALIFIER, Bytes.toBytes(queryState.name))
    put.addColumn(FAMILY, START_DATE_QUALIFIER, Bytes.toBytes(startDate))
    put.addColumn(FAMILY, ENGINE_QUALIFIER, Bytes.toBytes(engine))
    TsdbQueryMetrics.qualifiers.foreach { metricName =>
      val (count, time, speed) = metricValues.get(metricName) match {
        case Some(data) => (data.count, data.time, data.speed)
        case None       => (0L, 0L, 0d)
      }

      put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricCount), Bytes.toBytes(count))
      put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricTime), Bytes.toBytes(time))
      put.addColumn(FAMILY, Bytes.toBytes(metricName + "_" + metricSpeed), Bytes.toBytes(speed))

    }
    Using.resource(getTable)(_.put(put))
  }

  override def queriesByFilter(
      filter: Option[QueryMetricsFilter],
      limit: Option[Int] = None
  ): Iterator[TsdbQueryMetrics] = withTables {
    val results = rowsForFilter(filter).map(toMetric)

    val grouped = new GroupByIterator[TsdbQueryMetrics, String](_.queryId, results)
      .map(x => joinMetrics(x._2))

    grouped.take(limit.getOrElse(DEFAULT_LIMIT))
  }

  override def deleteMetrics(filter: QueryMetricsFilter): Int = {
    val table = getTable
    var n = 0
    rowsForFilter(Some(filter)).foreach { result =>
      table.delete(new Delete(result.getRow))
      n += 1
    }
    n
  }

  private def rowsForFilter(filter: Option[QueryMetricsFilter]): Iterator[Result] = {
    def setFilters(scan: Scan): Unit = {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filter.foreach { f =>
        f.queryState.foreach { queryState =>
          filterList.addFilter(
            new SingleColumnValueFilter(
              FAMILY,
              STATE_QUALIFIER,
              CompareOperator.EQUAL,
              Bytes.toBytes(queryState.name)
            )
          )
        }
      }
      if (!filterList.getFilters.isEmpty) {
        scan.setFilter(filterList)
      }
    }

    val scan = filter match {
      case Some(f) =>
        f.queryId match {
          case Some(queryId) =>
            val begin = Bytes.toBytes(queryId)
            val end = ClientUtil.calculateTheClosestNextRowKeyForPrefix(begin)
            val scan = new Scan().withStartRow(end, false).withStopRow(begin, true).addFamily(FAMILY).setReversed(true)
            setFilters(scan)
            scan

          case _ =>
            val scan = new Scan().addFamily(FAMILY).setReversed(true)
            setFilters(scan)
            scan
        }
      case None =>
        new Scan().addFamily(FAMILY).setReversed(true)
    }

    HBaseUtils.executeScan(connection, getTableName(namespace), scan, NoMetricCollector)
  }

  private def toMetric(result: Result): TsdbQueryMetrics = {
    val metrics = TsdbQueryMetrics.qualifiers.collect {
      case qualifier if result.containsColumn(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount)) =>
        qualifier -> {
          MetricData(
            Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricCount))),
            Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricTime))),
            Bytes.toDouble(result.getValue(FAMILY, Bytes.toBytes(qualifier + "_" + metricSpeed)))
          )
        }
    }.toMap
    val (qId, pId) = parseKey(result.getRow)
    TsdbQueryMetrics(
      queryId = qId,
      partitionId = pId,
      state = QueryStates.getByName(Bytes.toString(result.getValue(FAMILY, STATE_QUALIFIER))),
      engine = Bytes.toString(result.getValue(FAMILY, ENGINE_QUALIFIER)),
      query = Bytes.toString(result.getValue(FAMILY, QUERY_QUALIFIER)),
      startDate = OffsetDateTime.ofInstant(
        Instant.ofEpochMilli(Bytes.toLong(result.getValue(FAMILY, START_DATE_QUALIFIER))),
        ZoneOffset.UTC
      ),
      totalDuration = Bytes.toLong(result.getValue(FAMILY, TOTAL_DURATION_QUALIFIER)),
      metrics = metrics
    )
  }

  private def joinMetrics(metrics: Seq[TsdbQueryMetrics]): TsdbQueryMetrics = {
    case class Info(
        startTime: OffsetDateTime,
        stopTime: OffsetDateTime,
        state: QueryState,
        metrics: Map[String, MetricData]
    )
    val h = metrics.head

    if (metrics.size > 1) {
      val merged = {
        metrics.tail.foldLeft(
          Info(h.startDate, h.startDate.plusNanos(h.totalDuration.toInt), h.state, h.metrics)
        ) { (i, m) =>
          val end = m.startDate.plusNanos(m.totalDuration.toInt)
          i.copy(
            startTime = if (i.startTime.isBefore(m.startDate)) i.startTime else m.startDate,
            stopTime = if (i.stopTime.isAfter(end)) i.stopTime else end,
            state = QueryStates.combine(i.state, m.state),
            metrics = mergeMetrics(i.metrics, m.metrics)
          )
        }
      }

      val duration = Duration.between(merged.startTime, merged.stopTime).toNanos
      val ms = merged.metrics.map {
        case (k, v) =>
          (k, v.copy(speed = if (duration != 0d) v.count.toDouble / MetricCollector.asSeconds(duration) else 0d))
      }

      h.copy(
        startDate = merged.startTime,
        totalDuration = duration,
        state = merged.state,
        metrics = ms
      )
    } else h
  }

  private def mergeMetrics(a: Map[String, MetricData], b: Map[String, MetricData]): Map[String, MetricData] = {
    (a.keySet ++ b.keySet).map { k =>
      val m1 = a.getOrElse(k, MetricData(0L, 0L, 0d))
      val m2 = b.getOrElse(k, MetricData(0L, 0L, 0d))
      k -> MetricData(m1.count + m2.count, m1.time + m2.time, m1.speed + m2.speed)
    }.toMap
  }

  private def withTables[T](block: => T): T = {
    checkTablesExistsElseCreate()
    block
  }

  private def getTable: Table = connection.getTable(getTableName(namespace))

  private def rowKey(queryId: String, partitionId: Option[String]): Array[Byte] = {
    val key = partitionId.map(x => s"${queryId}_$x").getOrElse(queryId)
    Bytes.toBytes(key)
  }

  private def parseKey(bytes: Array[Byte]): (String, Option[String]) = {
    val strKey = Bytes.toString(bytes)
    val splitIndex = strKey.indexOf('_')
    if (splitIndex != -1) (strKey.substring(0, splitIndex), Some(strKey.substring(splitIndex + 1)))
    else (strKey, None)
  }

  private def checkTablesExistsElseCreate(): Unit = {
    try {
      val tableName = getTableName(namespace)
      Using.resource(connection.getAdmin) { admin =>
        if (!admin.tableExists(tableName)) {
          val desc = TableDescriptorBuilder
            .newBuilder(tableName)
            .setColumnFamilies(
              Seq(
                ColumnFamilyDescriptorBuilder.of(FAMILY),
                ColumnFamilyDescriptorBuilder.of(ID_FAMILY)
              ).asJavaCollection
            )
            .build()
          admin.createTable(desc)
        }
      }
    } catch {
      case _: TableExistsException =>
    }
  }
}
