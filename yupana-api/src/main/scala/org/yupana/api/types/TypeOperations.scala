package org.yupana.api.types

import org.joda.time.Period
import org.yupana.api.Time

import scala.reflect.ClassTag

case class TypeOperations[T](
  binaryOperations: Map[(String, String), BinaryOperation[T]],
  unaryOperations: Map[String, UnaryOperation[T]],
  aggregations: Map[String, Aggregation[T]],
  arrayOperations: Map[String, UnaryOperation[Array[T]]]
) {
  def biOperation[U](name: String, argType: DataType.Aux[U]): Option[BinaryOperation.Aux[T, U, _]] = {
    binaryOperations.get((name, argType.meta.sqlTypeName))
      .map(op => op.asInstanceOf[BinaryOperation.Aux[T, U, op.Out]])
  }
  def unaryOperation(name: String): Option[UnaryOperation[T]] = unaryOperations.get(name)
  def aggregation(name: String): Option[Aggregation[T]] = aggregations.get(name)
}

object TypeOperations {
  def intOperations[T : Integral](dt: DataType.Aux[T]): TypeOperations[T] = TypeOperations(
    BinaryOperation.integralOperations(dt),
    UnaryOperation.numericOperations(dt),
    Aggregation.intAggregations(dt),
    Map.empty
  )

  def fracOperations[T : Fractional](dt: DataType.Aux[T]): TypeOperations[T] = TypeOperations(
    BinaryOperation.fractionalOperations(dt),
    UnaryOperation.numericOperations(dt),
    Aggregation.fracAggregations(dt),
    Map.empty
  )

  def stringOperations(dt: DataType.Aux[String]): TypeOperations[String] = TypeOperations(
    BinaryOperation.stringOperations,
    UnaryOperation.stringOperations,
    Aggregation.stringAggregations,
    UnaryOperation.stringArrayOperations
  )

  def boolOperations(dt: DataType.Aux[Boolean]): TypeOperations[Boolean] = TypeOperations(
    Map.empty,
    Map.empty,
    Map.empty,
    Map.empty
  )

  def timeOperations(dt: DataType.Aux[Time]): TypeOperations[Time] = TypeOperations(
    BinaryOperation.timeOperations,
    UnaryOperation.timeOperations,
    Aggregation.timeAggregations,
    Map.empty
  )

  def periodOperations(dt: DataType.Aux[Period]): TypeOperations[Period] = TypeOperations(
    BinaryOperation.periodOperations,
    Map.empty,
    Map.empty,
    Map.empty
  )

  def tupleOperations[T, U](dtt: DataType.Aux[T], dtu: DataType.Aux[U]): TypeOperations[(T, U)] = {

    TypeOperations(
      BinaryOperation.tupleOperations(dtt.operations.binaryOperations, dtu.operations.binaryOperations),
      UnaryOperation.tupleOperations(dtt.operations.unaryOperations, dtu.operations.unaryOperations),
      Map.empty,
      Map.empty
    )
  }

  def arrayOperations[T](dtt: DataType.Aux[T])(implicit ct: ClassTag[T]): TypeOperations[Array[T]] = {
    TypeOperations(
      BinaryOperation.arrayOperations[T](dtt, ct),
      UnaryOperation.arrayOperations[T] ++ dtt.operations.arrayOperations,
      Map.empty,
      Map.empty
    )
  }
}
