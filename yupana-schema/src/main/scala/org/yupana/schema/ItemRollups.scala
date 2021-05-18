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

package org.yupana.schema

import org.yupana.api.query._
import org.yupana.api.schema.Rollup

object ItemRollups {

  import org.yupana.schema.ItemTableMetrics.ItemRollupFields._
  import org.yupana.schema.Tables._

  val itemDayRollup: Rollup = Rollup(
    name = "itemByDay",
    filter = None,
    groupBy = itemByDayTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRawData,
    fromTable = itemsKkmTable,
    toTable = itemByDayTable,
    timeExpr = TruncDayExpr(TimeExpr)
  )

  val itemWeekRollup: Rollup = Rollup(
    name = "itemByWeek",
    filter = None,
    groupBy = itemByWeekTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRollup,
    fromTable = itemByDayTable,
    toTable = itemByWeekTable,
    timeExpr = TruncWeekExpr(TimeExpr)
  )

  val itemMonthRollup: Rollup = Rollup(
    name = "itemByMonth",
    filter = None,
    groupBy = itemByMonthTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRollup,
    fromTable = itemByDayTable,
    toTable = itemByMonthTable,
    timeExpr = TruncMonthExpr(TimeExpr)
  )

  val itemKkmsDayRollup: Rollup = Rollup(
    name = "itemKkmsByDay",
    filter = None,
    groupBy = itemKkmsByDayTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRawData,
    fromTable = itemsKkmTable,
    toTable = itemKkmsByDayTable,
    timeExpr = TruncDayExpr(TimeExpr)
  )

  val itemKkmsWeekRollup: Rollup = Rollup(
    name = "itemKkmsByWeek",
    filter = None,
    groupBy = itemKkmsByWeekTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRollup,
    fromTable = itemKkmsByDayTable,
    toTable = itemKkmsByWeekTable,
    timeExpr = TruncWeekExpr(TimeExpr)
  )

  val itemKkmsMonthRollup: Rollup = Rollup(
    name = "itemKkmsByMonth",
    filter = None,
    groupBy = itemKkmsByMonthTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRollup,
    fromTable = itemKkmsByDayTable,
    toTable = itemKkmsByMonthTable,
    timeExpr = TruncMonthExpr(TimeExpr)
  )

  val kkmsItemDayRollup: Rollup = Rollup(
    name = "kkmsItemByDay",
    filter = None,
    groupBy = kkmsItemByDayTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRawData,
    fromTable = itemsKkmTable,
    toTable = kkmsItemByDayTable,
    timeExpr = TruncDayExpr(TimeExpr)
  )

  val kkmsItemWeekRollup: Rollup = Rollup(
    name = "kkmsItemByWeek",
    filter = None,
    groupBy = kkmsItemByWeekTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRollup,
    fromTable = kkmsItemByDayTable,
    toTable = kkmsItemByWeekTable,
    timeExpr = TruncWeekExpr(TimeExpr)
  )

  val kkmsItemMonthRollup: Rollup = Rollup(
    name = "kkmsItemByMonth",
    filter = None,
    groupBy = kkmsItemByMonthTable.dimensionSeq.map(d => DimensionExpr(d.aux)),
    fields = baseFields :+ countFromRollup,
    fromTable = itemByDayTable,
    toTable = kkmsItemByMonthTable,
    timeExpr = TruncMonthExpr(TimeExpr)
  )
}