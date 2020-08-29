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

import org.joda.time.Period
import org.yupana.api.Time
import org.yupana.core.model.InternalRow
import org.yupana.api.query._
import org.yupana.core.operations.Operations

object ExpressionCalculator {
  def evaluateConstant(expr: Expression): expr.Out = {
    assert(expr.kind == Const)
    eval(expr, null, null)
  }

  def preEvaluated(expr: Expression, queryContext: QueryContext, internalRow: InternalRow): expr.Out = {
    expr match {
      case ConstantExpr(v) => v.asInstanceOf[expr.Out]
      case _               => internalRow.get[expr.Out](queryContext, expr)
    }
  }

  def evaluateExpression(
      expr: Expression,
      queryContext: QueryContext,
      internalRow: InternalRow
  ): expr.Out = {
    val res = if (queryContext != null) {
      val idx = queryContext.exprsIndex.getOrElse(expr, -1)
      if (idx >= 0) {
        internalRow.get[expr.Out](idx)
      } else {
        null.asInstanceOf[expr.Out]
      }
    } else {
      null.asInstanceOf[expr.Out]
    }

    if (res == null) {
      eval(expr, queryContext, internalRow)
    } else {
      res
    }
  }

  def evaluateMap[I, M](expr: AggregateExpr.Aux[I, M, _], queryContext: QueryContext, row: InternalRow): M = {
    val res = expr match {
      case MinExpr(e) => row.get[M](queryContext, e)
      case MaxExpr(e) => row.get[M](queryContext, e)
      case SumExpr(e) => row.get[M](queryContext, e)
      case CountExpr(e) =>
        val v = row.get[I](queryContext, e)
        if (v != null) 1L else 0L
      case DistinctCountExpr(e) =>
        val v = row.get[I](queryContext, e)
        if (v != null) Set(v) else Set.empty[I]
      case DistinctRandomExpr(e) =>
        val v = row.get[I](queryContext, e)
        if (v != null) Set(v) else Set.empty[I]
    }

    res.asInstanceOf[M]
  }

  def evaluateReduce[M](
      expr: AggregateExpr.Aux[_, M, _],
      queryContext: QueryContext,
      a: InternalRow,
      b: InternalRow
  ): M = {
    def reduce(x: M, y: M): M = {
      val res = expr match {
        case m @ MinExpr(_)        => m.ord.min(x, y)
        case m @ MaxExpr(_)        => m.ord.max(x, y)
        case s @ SumExpr(_)        => s.numeric.plus(x, y)
        case CountExpr(_)          => x.asInstanceOf[Long] + y.asInstanceOf[Long]
        case DistinctCountExpr(_)  => x.asInstanceOf[Set[_]] ++ y.asInstanceOf[Set[_]]
        case DistinctRandomExpr(_) => x.asInstanceOf[Set[_]] ++ y.asInstanceOf[Set[_]]
      }

      res.asInstanceOf[M]
    }

    val aValue = a.get[M](queryContext, expr)
    val bValue = b.get[M](queryContext, expr)

    if (aValue != null) {
      if (bValue != null) {
        reduce(aValue, bValue)
      } else aValue
    } else bValue
  }

  def evaluatePostMap[M, O](expr: AggregateExpr.Aux[_, M, O], queryContext: QueryContext, row: InternalRow): O = {
    val oldValue = row.get[M](queryContext, expr)

    val res = expr match {
      case MinExpr(_)           => oldValue
      case MaxExpr(_)           => oldValue
      case s @ SumExpr(_)       => if (oldValue != null) oldValue else s.numeric.zero
      case CountExpr(_)         => oldValue
      case DistinctCountExpr(_) => oldValue.asInstanceOf[Set[_]].size
      case DistinctRandomExpr(_) =>
        val s = oldValue.asInstanceOf[Set[_]]
        val n = util.Random.nextInt(s.size)
        s.iterator.drop(n).next
    }

    res.asInstanceOf[O]
  }

  private def eval(expr: Expression, qc: QueryContext, row: InternalRow): expr.Out = {

    val res = expr match {
      case ConstantExpr(x) => x //.asInstanceOf[expr.Out]

      case TimeExpr                     => null
      case DimensionExpr(_)             => null
      case DimensionIdExpr(_)           => null
      case MetricExpr(_)                => null
      case LinkExpr(_, _)               => null
      case ae: AggregateExpr[_, _]      => evaluateExpression(ae.expr, qc, row)
      case we: WindowFunctionExpr[_, _] => evaluateExpression(we.expr, qc, row)

      case ConditionExpr(condition, positive, negative) =>
        val x = evaluateExpression(condition, qc, row)
        if (x) {
          evaluateExpression(positive, qc, row)
        } else {
          evaluateExpression(negative, qc, row)
        }

      case TruncYearExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncYear)

      case TruncMonthExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncMonth)

      case TruncDayExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncDay)

      case TruncWeekExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncWeek)

      case TruncHourExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncHour)

      case TruncMinuteExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncMinute)

      case TruncSecondExpr(e) =>
        evaluateUnary(qc, row)(e, Operations.truncSecond)

      case p @ PlusExpr(a, b)    => evaluateBinary(qc, row)(a, b, p.numeric.plus)
      case m @ MinusExpr(a, b)   => evaluateBinary(qc, row)(a, b, m.numeric.minus)
      case t @ TimesExpr(a, b)   => evaluateBinary(qc, row)(a, b, t.numeric.times)
      case d @ DivIntExpr(a, b)  => evaluateBinary(qc, row)(a, b, d.integral.quot)
      case d @ DivFracExpr(a, b) => evaluateBinary(qc, row)(a, b, d.fractional.div)

      case ConcatExpr(a, b) => evaluateBinary(qc, row)(a, b, (x: String, y: String) => x + y)

      case EqExpr(a, b)     => evaluateBinary(qc, row)(a, b, (x: a.Out, y: b.Out) => x == y)
      case NeqExpr(a, b)    => evaluateBinary(qc, row)(a, b, (x: a.Out, y: b.Out) => x != y)
      case e @ GtExpr(a, b) => evaluateBinary(qc, row)(a, b, e.ordering.gt)
      case e @ LtExpr(a, b) => evaluateBinary(qc, row)(a, b, e.ordering.lt)
      case e @ GeExpr(a, b) => evaluateBinary(qc, row)(a, b, e.ordering.gteq)
      case e @ LeExpr(a, b) => evaluateBinary(qc, row)(a, b, e.ordering.lteq)

      case IsNullExpr(e)    => evaluateExpression(e, qc, row) == null
      case IsNotNullExpr(e) => evaluateExpression(e, qc, row) != null

      case TypeConvertExpr(tc, e) =>
        tc.convert(evaluateExpression(e, qc, row))

      case InExpr(e, vs) =>
        vs contains evaluateExpression(e, qc, row)

      case NotInExpr(e, vs) =>
        val eValue = evaluateExpression(e, qc, row)
        eValue != null && !vs.contains(eValue)

      case AndExpr(cs) =>
        val executed = cs.map(c => evaluateExpression(c, qc, row))
        executed.reduce((a, b) => a && b)

      case OrExpr(cs) =>
        val executed = cs.map(c => evaluateExpression(c, qc, row))
        executed.reduce((a, b) => a || b)

      case TupleExpr(e1, e2) =>
        (evaluateExpression(e1, qc, row), evaluateExpression(e2, qc, row))

      case LowerExpr(e)  => evaluateUnary(qc, row)(e, (x: String) => x.toLowerCase)
      case UpperExpr(e)  => evaluateUnary(qc, row)(e, (x: String) => x.toUpperCase)
      case LengthExpr(e) => evaluateUnary(qc, row)(e, (x: String) => x.length)

      case a @ AbsExpr(e) => evaluateUnary(qc, row)(e, a.numeric.abs)

      case NotExpr(e) => evaluateUnary(qc, row)(e, (x: Boolean) => !x)

      case TimeMinusExpr(a, b) => evaluateBinary(qc, row)(a, b, (x: Time, y: Time) => math.abs(x.millis - y.millis))
      case TimeMinusPeriodExpr(a, b) =>
        evaluateBinary(qc, row)(a, b, (t: Time, p: Period) => Time(t.toDateTime.minus(p).getMillis))
      case TimePlusPeriodExpr(a, b) =>
        evaluateBinary(qc, row)(a, b, (t: Time, p: Period) => Time(t.toDateTime.minus(p).getMillis))
      case PeriodPlusPeriodExpr(a, b) => evaluateBinary(qc, row)(a, b, (x: Period, y: Period) => x plus y)

      case ce @ ContainsExpr(a, v) => evaluateBinary(qc, row)(a, v, (ar: ce.In, x: ce.Item) => ar.contains(x))
//      case ContainsAllExpr(a, vs) =>
//      case ContainsAnyExpr(a, vs) =>
//      case ContainsSameExpr(a, vs) =>

      case ae @ ArrayExpr(es) =>
        val values: Array[ae.elementDataType.T] =
          Array.ofDim[ae.elementDataType.T](es.length)(ae.elementDataType.classTag)
        var success = true
        var i = 0

        while (i < es.length && success) {
          val v = evaluateExpression(es(i), qc, row)
          values(i) = v
          if (v == null) {
            success = false
          }
          i += 1
        }

        if (success) values else null

//      case x => throw new IllegalArgumentException(s"Unsupported expression $x")
    }

    // I cannot find a better solution to ensure compiler that concrete expr type Out is the same with expr.Out
    res.asInstanceOf[expr.Out]
  }

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O = {
    winFuncExpr match {
      case LagExpr(_) => if (index > 0) values(index - 1).asInstanceOf[O] else null.asInstanceOf[O]
    }
  }

  private def evaluateUnary[A, O](qc: QueryContext, internalRow: InternalRow)(e: Expression.Aux[A], f: A => O): O = {
    val ev = evaluateExpression(e, qc, internalRow)
    if (ev != null) f(ev) else null.asInstanceOf[O]
  }

  private def evaluateBinary[A, B, O](
      qc: QueryContext,
      internalRow: InternalRow
  )(a: Expression.Aux[A], b: Expression.Aux[B], f: (A, B) => O): O = {
    val left = evaluateExpression(a, qc, internalRow)
    val right = evaluateExpression(b, qc, internalRow)
    if (left != null && right != null) {
      f(left, right)
    } else {
      null.asInstanceOf[O]
    }
  }

  private def contains[T](a: Array[T], t: T): Boolean = a contains t
  private def containsAll[T](a: Array[T], b: Array[T]): Boolean = b.forall(a.contains)
  private def containsAny[T](a: Array[T], b: Array[T]): Boolean = b.exists(a.contains)
  private def containsSame[T](a: Array[T], b: Array[T]): Boolean = a sameElements b
}
