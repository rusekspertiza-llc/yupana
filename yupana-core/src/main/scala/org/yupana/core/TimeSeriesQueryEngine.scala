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

package org.yupana.core
import org.yupana.api.query.{ DataPoint, Query, Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.core.utils.Explanation

class TimeSeriesQueryEngine(tsdb: TSDB) {
  def query(query: Query): Result = {
    tsdb.query(query)
  }

  def explain(query: Query): Result = {
    val steps = tsdb.explain(query)

    val filter: Explanation => Boolean = !_.trace

    SimpleResult(
      "Explanation",
      TimeSeriesQueryEngine.explainColumns,
      TimeSeriesQueryEngine.explainTypes,
      steps.withFilter(filter).map(e => Array[Any](e.component, e.message)).iterator
    )
  }

  def put(dps: Seq[DataPoint]): Unit = {
    tsdb.put(dps.iterator)
  }
}

object TimeSeriesQueryEngine {
  private val explainColumns = Seq("COMPONENT", "MESSAGE")
  private val explainTypes = explainColumns.map(_ => DataType[String])
}
