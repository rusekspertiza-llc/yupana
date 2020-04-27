package org.yupana.hbase

import java.nio.ByteBuffer

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.{ Cell, CellUtil }
import org.apache.hadoop.hbase.client.{ Result => HBaseResult }
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.Time
import org.yupana.api.schema.{ RawDimension, Table }
import org.yupana.api.types.DataType
import org.yupana.core.model.{ InternalRow, InternalRowBuilder }
import org.yupana.hbase.HBaseUtils.TAGS_POSITION_IN_ROW_KEY

import scala.collection.AbstractIterator

class TSDHBaseRowIterator(
    context: InternalQueryContext,
    rows: Iterator[HBaseResult],
    internalRowBuilder: InternalRowBuilder
) extends AbstractIterator[InternalRow]
    with StrictLogging {

  val dimensions = context.table.dimensionSeq.toArray

  val maxFamiliesCount = context.table.metrics.map(_.group).distinct.size

  val offsets = Array.ofDim[Int](maxFamiliesCount)
  val endOffsets = Array.ofDim[Int](maxFamiliesCount)

  var cells = Array.empty[Cell]
  var familiesCount = 0
  var currentTime = Long.MaxValue
  var currentRowKey = Array.empty[Byte]

  override def hasNext: Boolean = {
    rows.hasNext || currentTime != Long.MaxValue
  }

  override def next(): InternalRow = {
    if (rows.hasNext && currentTime == Long.MaxValue) {
      nextHBaseRow()
    } else if (rows.isEmpty && currentTime == Long.MaxValue) {
      throw new IllegalStateException("Next on empty iterator")
    }

    currentTime = nextDatapoint()

    internalRowBuilder.buildAndReset()
  }

  def nextHBaseRow(): Unit = {
    val result = rows.next()
    currentRowKey = result.getRow
    cells = result.rawCells()
    familiesCount = fillFamiliesOffsets(cells)

    def findMinTime(): Long = {
      var min = Long.MaxValue
      var i = 0
      while (i < familiesCount) {
        min = math.min(min, getTimeOffset(cells(offsets(i))))
        i += 1
      }
      min
    }

    currentTime = findMinTime()
  }

  def nextDatapoint() = {
    loadRow(currentRowKey)
    var nextMinTime = Long.MaxValue
    var i = 0
    while (i < familiesCount) {
      val offset = offsets(i)
      val cell = cells(offset)
      if (getTimeOffset(cell) == currentTime) {
        loadCell(cell)
        if (offset < endOffsets(i)) {
          nextMinTime = math.min(nextMinTime, getTimeOffset(cells(offset + 1)))
          offsets(i) = offset + 1
        }
      }
      i += 1
    }
    nextMinTime
  }

  private def loadRow(rowKey: Array[Byte]) = {
    val baseTime = Bytes.toLong(rowKey)
    internalRowBuilder.set(Some(Time(baseTime + currentTime)))
    var i = 0
    val bb = ByteBuffer.wrap(rowKey, TAGS_POSITION_IN_ROW_KEY, rowKey.length - TAGS_POSITION_IN_ROW_KEY)
    dimensions.foreach { dim =>
      val value = dim.storable.read(bb)
      if (dim.isInstanceOf[RawDimension[_]]) {
        internalRowBuilder.set((Table.DIM_TAG_OFFSET + i).toByte, Some(value))
      }
      i += 1
    }
  }

  private def loadCell(cell: Cell): Boolean = {
    val bb = ByteBuffer.wrap(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    var correct = true
    while (bb.hasRemaining && correct) {
      val tag = bb.get()
      context.fieldForTag(tag) match {
        case Some(Left(metric)) =>
          val v = metric.dataType.storable.read(bb)
          internalRowBuilder.set(tag, Some(v))
        case Some(Right(_)) =>
          val v = DataType.stringDt.storable.read(bb)
          internalRowBuilder.set(tag, Some(v))
        case None =>
          logger.warn(s"Unknown tag: $tag, in table: ${context.table.name}")
          correct = false
      }
    }
    correct
  }

  private def fillFamiliesOffsets(cells: Array[Cell]): Int = {
    var i = 1
    var j = 1
    var prevFamilyCell = cells(0)
    offsets(0) = 0
    while (i < cells.length) {
      val cell = cells(i)
      endOffsets(j - 1) = i - 1
      if (!CellUtil.matchingFamily(prevFamilyCell, cell)) {
        prevFamilyCell = cell
        offsets(j) = i
        j += 1
      }
      i += 1
    }
    endOffsets(j - 1) = cells.length - 1

    j
  }

  private def getTimeOffset(cell: Cell): Long = {
    Bytes.toLong(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
  }
}
