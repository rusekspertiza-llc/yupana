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

package org.yupana.core.utils

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.core.{ ExpressionCalculator, QueryOptimizer }
import org.yupana.core.utils.ConditionMatchers._

import scala.collection.mutable.ListBuffer

case class TimeBoundedCondition(from: Option[Long], to: Option[Long], conditions: Seq[Condition]) {
  def toCondition: Condition = {
    import org.yupana.api.query.syntax.All._

    QueryOptimizer.simplifyCondition(
      AndExpr(
        Seq(
          from.map(f => ge(time, const(Time(f)))).getOrElse(ConstantExpr(true).aux),
          to.map(t => lt(time, const(Time(t)))).getOrElse(ConstantExpr(true).aux)
        ) ++ conditions
      )
    )
  }
}

object TimeBoundedCondition {

  def apply(expressionCalculator: ExpressionCalculator, condition: Condition): Seq[TimeBoundedCondition] = {
    condition match {
      case a: AndExpr => andToTimeBounded(expressionCalculator)(a)
      case x          => Seq(TimeBoundedCondition(None, None, Seq(x)))
    }
  }

  def apply(from: Long, to: Long, condition: Condition): TimeBoundedCondition = {
    QueryOptimizer.simplifyCondition(condition) match {
      case AndExpr(cs) => TimeBoundedCondition(Some(from), Some(to), cs)
      case o: OrExpr   => throw new IllegalArgumentException(s"Or not supported yet $o")
      case c           => TimeBoundedCondition(Some(from), Some(to), Seq(c))
    }
  }

  def toCondition(conditions: Seq[TimeBoundedCondition]): Condition = {
    conditions.map(_.toCondition) match {
      case Nil    => ConstantExpr(true)
      case Seq(x) => x
      case xs     => OrExpr(xs)
    }
  }

  def merge(conditions: Seq[TimeBoundedCondition]): TimeBoundedCondition = {
    if (conditions.isEmpty) throw new IllegalArgumentException("Conditions must not be empty")

    val from = conditions.head.from
    val to = conditions.head.to
    val cs = conditions.foldLeft(Seq.empty[Condition])((a, c) =>
      if (c.from == from && c.to == to) {
        a ++ c.conditions
      } else {
        throw new IllegalArgumentException("Conditions must have same time limits.")
      }
    )
    TimeBoundedCondition(from, to, cs)
  }

  private def andToTimeBounded(expressionCalculator: ExpressionCalculator)(and: AndExpr): Seq[TimeBoundedCondition] = {
    var from = Option.empty[Long]
    var to = Option.empty[Long]
    val other = ListBuffer.empty[Condition]

    def updateFrom(c: Condition, e: Expression, offset: Long): Unit = {
      val const = expressionCalculator.evaluateConstant(e.asInstanceOf[Expression.Aux[Time]])
      if (const != null) {
        from = from.map(o => math.max(const.millis + offset, o)) orElse Some(const.millis + offset)
      } else {
        other += c
      }
    }

    def updateTo(c: Condition, e: Expression, offset: Long): Unit = {
      val const = expressionCalculator.evaluateConstant(e.asInstanceOf[Expression.Aux[Time]])
      if (const != null) {
        to = to.map(o => math.max(const.millis + offset, o)) orElse Some(const.millis)
      } else {
        other += c
      }
    }

    and.conditions.foreach {
      case c @ Gt(TimeExpr, e) => updateFrom(c, e, 1L)
      case c @ Lt(e, TimeExpr) => updateFrom(c, e, 1L)

      case c @ Ge(TimeExpr, e) => updateFrom(c, e, 0L)
      case c @ Le(e, TimeExpr) => updateFrom(c, e, 0L)

      case c @ Lt(TimeExpr, e) => updateTo(c, e, 0L)
      case c @ Gt(e, TimeExpr) => updateTo(c, e, 0L)

      case c @ Le(TimeExpr, e) => updateTo(c, e, 1L)
      case c @ Ge(e, TimeExpr) => updateTo(c, e, 1L)

      case c @ (AndExpr(_) | OrExpr(_)) => throw new IllegalArgumentException(s"Unexpected condition $c")

      case x => other += x
    }

    Seq(TimeBoundedCondition(from, to, other.toList))
  }
}
