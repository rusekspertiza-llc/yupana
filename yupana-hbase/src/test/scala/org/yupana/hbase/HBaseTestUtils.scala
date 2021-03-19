package org.yupana.hbase

import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.yupana.api.types.DataType

import scala.collection.JavaConverters._
import scala.reflect.api
import scala.reflect.api.{ TypeCreator, Universe }
import scala.reflect.runtime.universe._

object HBaseTestUtils {
  def dimAHash(s: String): (Int, Long) = {
    (s.hashCode, UUID.nameUUIDFromBytes(s.getBytes(StandardCharsets.UTF_8)).getMostSignificantBits)
  }

  def row[A: TypeTag](baseTime: Long, a: A): RowBuilder = {
    val key = Bytes.toBytes(baseTime) ++ toBytes(a)
    new RowBuilder(key, Nil)
  }

  def row[A: TypeTag, B: TypeTag](baseTime: Long, a: A, b: B): RowBuilder = {
    val key = Bytes.toBytes(baseTime) ++ toBytes(a) ++ toBytes(b)
    new RowBuilder(key, Nil)
  }

  def row[A: TypeTag, B: TypeTag, C: TypeTag](baseTime: Long, a: A, b: B, c: C): RowBuilder = {
    val key = Bytes.toBytes(baseTime) ++ toBytes(a) ++ toBytes(b) ++ toBytes(c)
    new RowBuilder(key, Nil)
  }

  private lazy val mirror = runtimeMirror(getClass.getClassLoader)

  private def typeToTag[T](tpe: Type): TypeTag[T] = {
    TypeTag(
      mirror,
      new TypeCreator {
        override def apply[U <: Universe with Singleton](m: api.Mirror[U]): U#Type = {
          if (m eq mirror) {
            tpe.asInstanceOf[U#Type]
          } else throw new IllegalArgumentException("Wrong mirror")
        }
      }
    )
  }

  private def toBytes[T](t: T)(implicit typeTag: TypeTag[T]): Array[Byte] = {
    t match {
      case x: Long  => Bytes.toBytes(x)
      case x: Short => Bytes.toBytes(x)
      case x: Byte  => Bytes.toBytes(x)
      case x: Int   => Bytes.toBytes(x)
      case x: (_, _) =>
        val List(aTpe, bTpe) = typeTag.tpe.typeArgs
        toBytes(x._1)(typeToTag(aTpe)) ++ toBytes(x._2)(typeToTag(bTpe))
      case _ => throw new IllegalArgumentException(s"Unsupported type ${typeTag.tpe}")
    }
  }

  class RowBuilder(key: Array[Byte], cells: List[(String, Long, Array[Byte])]) {

    def cell(family: String, time: Long): RowBuilder = {
      new RowBuilder(key, (family, time, Array.empty[Byte]) :: cells)
    }

    def field(tag: Int, value: Long): RowBuilder = {
      field(tag, DataType.intDt[Long].storable.write(value))
    }

    def field(tag: Int, value: String): RowBuilder = {
      field(tag, DataType.stringDt.storable.write(value))
    }

    def field(tag: Int, value: Double): RowBuilder = {
      field(tag, DataType.fracDt[Double].storable.write(value))
    }

    def field(tag: Int, value: BigDecimal): RowBuilder = {
      field(tag, DataType.fracDt[BigDecimal].storable.write(value))
    }

    def field(tag: Int, value: Array[Byte]): RowBuilder = {
      val f = tag.toByte +: value
      val (family, time, bytes) = cells.head
      new RowBuilder(key, (family, time, bytes ++ f) :: cells.tail)
    }

    def hbaseRow: Result = {
      val cs: List[Cell] = cells.reverse.map {
        case (family, time, value) =>
          TestCell(
            key,
            Bytes.toBytes(family),
            Bytes.toBytes(time),
            value
          )
      }

      Result.create(cs.asJava)
    }
  }

  case class TestCell(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) extends Cell {
    override def getRowArray: Array[Byte] = row
    override def getRowOffset = 0
    override def getRowLength: Short = row.length.toShort
    override def getFamilyArray: Array[Byte] = family
    override def getFamilyOffset = 0
    override def getFamilyLength = family.length.toByte
    override def getQualifierArray: Array[Byte] = qualifier
    override def getQualifierOffset = 0
    override def getQualifierLength = qualifier.length
    override def getTimestamp = 0L
    override def getTypeByte = 0
    override def getValueArray: Array[Byte] = value
    override def getValueOffset = 0
    override def getValueLength = value.length
    override def getTagsArray: Array[Byte] = null
    override def getTagsOffset = 0
    override def getSequenceId = 0L
    override def getTagsLength = 0
    override def getSerializedSize: Int = 0
    override def heapSize(): Long = 0L
  }
}
