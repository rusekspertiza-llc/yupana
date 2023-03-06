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

import org.apache.hadoop.hbase.client.{ Connection, Result => HResult }
import org.yupana.api.query.DataPoint
import org.yupana.api.schema.{ Dimension, Schema }
import org.yupana.core.dao.DictionaryProvider
import org.yupana.core.model.UpdateInterval
import org.yupana.core.utils.{ Explanation, Writer }
import org.yupana.core.utils.Explanation.Explained
import org.yupana.core.utils.metric.MetricQueryCollector
import org.yupana.core.{ IteratorMapReducible, MapReducible }
import org.yupana.hbase.HBaseUtils._

class TSDaoHBase(
    override val schema: Schema,
    connection: Connection,
    namespace: String,
    override val dictionaryProvider: DictionaryProvider,
    putsBatchSize: Int = TSDaoHBaseBase.PUTS_BATCH_SIZE,
    reduceLimit: Int
) extends TSDaoHBaseBase[Iterator] {

  override def mapReduceEngine(metricQueryCollector: MetricQueryCollector): MapReducible[Iterator] =
    new IteratorMapReducible(reduceLimit)

  override def executeScans(
      queryContext: InternalQueryContext,
      intervals: Seq[(Long, Long)],
      rangeScanDims: Iterator[Map[Dimension, Seq[_]]]
  ): Explained[Iterator[HResult]] = {

    if (rangeScanDims.nonEmpty) {
      Writer.flatTraverseIterator(rangeScanDims) { dimIds =>
        queryContext.metricsCollector.createScans.measure(intervals.size) {
          Writer.flatTraverseIterator(intervals.iterator) {
            case (from, to) =>
              val filter = multiRowRangeFilter(queryContext.table, from, to, dimIds)
              createScan(queryContext, filter, Seq.empty, from, to).map {
                case Some(scan) =>
                  executeScan(connection, namespace, scan, queryContext, TSDaoHBaseBase.EXTRACT_BATCH_SIZE)
                case None => Iterator.empty[HResult]
              }
          }
        }
      }
    } else {
      Explanation.of(Iterator.empty)
    }
  }

  override def putBatch(username: String)(dataPointsBatch: Seq[DataPoint]): Seq[UpdateInterval] = {
    doPutBatch(connection, dictionaryProvider, namespace, username, putsBatchSize, dataPointsBatch)
  }
}
