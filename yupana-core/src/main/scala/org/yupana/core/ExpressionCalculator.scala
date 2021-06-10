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

import com.typesafe.scalalogging.StrictLogging
import org.joda.time.DateTimeFieldType
import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._
import org.yupana.api.types.DataType.TypeKind
import org.yupana.api.types.{ ArrayDataType, DataType, TupleDataType }
import org.yupana.api.utils.Tokenizer
import org.yupana.core.model.InternalRow

import scala.collection.AbstractIterator

trait ExpressionCalculator extends Serializable {
  def evaluateFilter(tokenizer: Tokenizer, internalRow: InternalRow): Boolean
  def evaluateExpressions(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluateMap(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluateReduce(tokenizer: Tokenizer, a: InternalRow, b: InternalRow): InternalRow
  def evaluatePostMap(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluatePostAggregateExprs(tokenizer: Tokenizer, internalRow: InternalRow): InternalRow
  def evaluatePostFilter(tokenizer: Tokenizer, row: InternalRow): Boolean

  def evaluateWindow[I, O](winFuncExpr: WindowFunctionExpr[I, O], values: Array[I], index: Int): O = {
    winFuncExpr match {
      case LagExpr(_) => if (index > 0) values(index - 1).asInstanceOf[O] else null.asInstanceOf[O]
    }
  }
}

object ExpressionCalculator extends StrictLogging {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.currentMirror
  import scala.tools.reflect.ToolBox

  private val tokenizer = TermName("tokenizer")
  private val calculator = q"_root_.org.yupana.core.ExpressionCalculator"

  case class State(
      index: Map[Expression[_], Int],
      required: Set[Expression[_]],
      globalDecls: Map[TermName, Tree],
      localDecls: Seq[(Expression[_], TermName, Tree)],
      trees: Seq[Tree]
  ) {
    def withRequired(e: Expression[_]): State = copy(required = required + e).withExpr(e)

    def withExpr(e: Expression[_]): State = {
      if (index.contains(e)) this else this.copy(index = this.index + (e -> index.size))
    }

    def withDefine(row: TermName, e: Expression[_], v: Tree): State = {
      if (required.contains(e)) {
        val newState = withExpr(e)
        val idx = newState.index(e)
        newState.copy(trees = q"$row.set($idx, $v)" +: trees)
      } else if (!localDecls.exists(_._1 == e)) {
        val entry: (Expression[_], TermName, Tree) = (e, nextLocalName, v)
        copy(localDecls = entry +: localDecls)
      } else {
        this
      }
    }

    def withDefineIf(row: TermName, e: Expression[_], cond: Tree, v: Tree): State = {
      if (required.contains(e)) {
        val newState = withExpr(e)
        val idx = newState.index(e)
        val tree = q"if ($cond) $row.set($idx, $v)"
        newState.withExpr(e).copy(trees = tree +: trees)
      } else if (!localDecls.exists(_._1 == e)) {
        val tpe = mkType(e)
        val tree = q"if ($cond) $v else null.asInstanceOf[$tpe]"
        val entry: (Expression[_], TermName, Tree) = (e, nextLocalName, tree)
        copy(localDecls = entry +: localDecls)
      } else {
        this
      }
    }

    def withGlobal(name: TermName, tpe: Tree, tree: Tree): State = {
      if (!globalDecls.contains(name))
        copy(globalDecls = globalDecls + (name -> q"private val $name: $tpe = $tree"))
      else this
    }

    def appendLocal(ts: Tree*): State = copy(trees = trees ++ ts)

    def fresh: (Tree, State) = {
      val locals = localDecls.reverse.map {
        case (e, n, v) =>
          val tpe = mkType(e)
          q"val $n: $tpe = $v"
      }

      val res =
        q"""
            ..$locals
            ..$trees
          """

      val newState = State(index, required, globalDecls, Seq.empty, Seq.empty)

      res -> newState
    }

    private def nextLocalName: TermName = TermName(s"l_${localDecls.size}")
  }

  private def className(dataType: DataType): String = {
    val tpe = dataType.classTag.toString()
    val lastDot = tpe.lastIndexOf(".")
    tpe.substring(lastDot + 1)
  }

  private def mkType(dataType: DataType): Tree = {
    dataType.kind match {
      case TypeKind.Regular => Ident(TypeName(className(dataType)))
      case TypeKind.Tuple =>
        val tt = dataType.asInstanceOf[TupleDataType[_, _]]
        AppliedTypeTree(
          Ident(TypeName("Tuple2")),
          List(Ident(TypeName(className(tt.aType))), Ident(TypeName(className(tt.bType))))
        )
      case TypeKind.Array =>
        val at = dataType.asInstanceOf[ArrayDataType[_]]
        AppliedTypeTree(
          Ident(TypeName("Seq")),
          List(Ident(TypeName(className(at.valueType))))
        )
    }
  }

  private def mkType(e: Expression[_]): Tree = {
    mkType(e.dataType)
  }

  private def mapValue(tpe: DataType)(v: Any): Tree = {
    import scala.reflect.classTag

    tpe.kind match {
      case TypeKind.Regular if tpe.classTag == classTag[Time] =>
        val t = v.asInstanceOf[Time]
        q"_root_.org.yupana.api.Time(${t.millis})"

      case TypeKind.Regular if tpe.classTag == classTag[BigDecimal] =>
        val str = v.asInstanceOf[BigDecimal].toString()
        q"""_root_.scala.math.BigDecimal($str)"""

      case TypeKind.Regular => Literal(Constant(v))

      case TypeKind.Tuple =>
        val tt = tpe.asInstanceOf[TupleDataType[_, _]]
        val (a, b) = v.asInstanceOf[(_, _)]
        Apply(Ident(TermName("Tuple2")), List(mapValue(tt.aType)(a), mapValue(tt.bType)(b)))

      case TypeKind.Array => throw new IllegalAccessException("Didn't expect to see Array here")
    }
  }

  private def mkGet(state: State, row: TermName, e: Expression[_]): Tree = {
    val tpe = mkType(e)
    e match {
      case ConstantExpr(x) =>
        val lit = mapValue(e.dataType)(x)
        q"$lit.asInstanceOf[$tpe]"
      case ae @ ArrayExpr(exprs) if e.kind == Const =>
        val lits = exprs.map {
          case ConstantExpr(v) => mapValue(ae.elementDataType)(v)
          case x               => throw new IllegalArgumentException(s"Unexpected constant expression $x")
        }
        val innerTpe = mkType(ae.elementDataType)
        q"Seq[$innerTpe](..$lits)"
      case x if state.index.contains(x) =>
        val idx = state.index(x)
        q"$row.get[$tpe]($idx)"

      case x =>
        state.localDecls
          .find(_._1 == x)
          .map(x => q"${x._2}")
          .getOrElse(throw new IllegalArgumentException(s"Unknown expression $x"))
    }
  }

  private def mkIsDefined(state: State, row: TermName, e: Expression[_]): Option[Tree] = {
    e match {
      case ConstantExpr(_)  => None
      case TimeExpr         => None
      case DimensionExpr(_) => None
      case _ =>
        state.index
          .get(e)
          .map(idx => q"$row.isDefined($idx)")
          .orElse(state.localDecls.find(_._1 == e).map(x => q"${x._2} != null"))
    }
  }

  private def mkSetUnary(
      row: TermName,
      state: State,
      e: Expression[_],
      a: Expression[_],
      f: Tree => Tree,
      elseTree: Option[Tree] = None
  ): State = {
    val newState = mkSet(row, state, a)
    val getA = mkGet(newState, row, a)
    val tpe = mkType(e)
    val v = f(getA)
    mkIsDefined(newState, row, a) match {
      case Some(aIsDefined) =>
        elseTree match {
          case Some(d) =>
            newState.withDefine(row, e, q"if ($aIsDefined) $v.asInstanceOf[$tpe] else $d.asInstanceOf[$tpe]")

          case None =>
            newState.withDefineIf(row, e, aIsDefined, q"$v.asInstanceOf[$tpe]")
        }
      case None =>
        newState.withDefine(row, e, q"$v.asInstanceOf[$tpe]")
    }
  }

  def mkSetMathUnary(
      row: TermName,
      state: State,
      e: Expression[_],
      a: Expression[_],
      fun: TermName
  ): State = {
    val aType = mkType(a)
    if (a.dataType.integral.nonEmpty) {
      mkSetUnary(row, state, e, a, x => q"${integralValName(a.dataType)}.$fun($x)")
        .withGlobal(
          integralValName(a.dataType),
          tq"Integral[$aType]",
          q"DataType.bySqlName(${e.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].integral.get"
        )
    } else {
      mkSetUnary(row, state, e, a, x => q"${fractionalValName(a.dataType)}.$fun($x)")
        .withGlobal(
          fractionalValName(a.dataType),
          tq"Fractional[$aType]",
          q"DataType.bySqlName(${e.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].fractional.get"
        )
    }
  }

  private def mkSetTypeConvertExpr(
      row: TermName,
      state: State,
      e: Expression[_],
      a: Expression[_]
  ): State = {
    val mapper = typeConverters.getOrElse(
      (a.dataType.meta.sqlTypeName, e.dataType.meta.sqlTypeName),
      throw new IllegalArgumentException(s"Unsupported type conversion ${a.dataType} to ${e.dataType}")
    )
    mkSetUnary(row, state, e, a, mapper)
  }

  private def mkSetBinary(
      row: TermName,
      state: State,
      e: Expression[_],
      a: Expression[_],
      b: Expression[_],
      f: (Tree, Tree) => Tree
  ): State = {
    val newState = mkSetExprs(row, state, Seq(a, b))

    val getA = mkGet(newState, row, a)
    val getB = mkGet(newState, row, b)
    val tpe = mkType(e)
    val v = q"${f(getA, getB)}.asInstanceOf[$tpe]"

    (mkIsDefined(newState, row, a), mkIsDefined(newState, row, b)) match {
      case (Some(aDefined), Some(bDefined)) =>
        newState.withDefineIf(row, e, q"$aDefined && $bDefined", v)
      case (Some(aDefined), None) =>
        newState.withDefineIf(row, e, q"$aDefined", v)
      case (None, Some(bDefined)) =>
        newState.withDefineIf(row, e, q"$bDefined", v)
      case (None, None) =>
        newState.withDefine(row, e, v)
    }
  }

  private def tcEntry[A, B](
      aToB: Tree => Tree
  )(implicit a: DataType.Aux[A], b: DataType.Aux[B]): ((String, String), Tree => Tree) = {
    ((a.meta.sqlTypeName, b.meta.sqlTypeName), aToB)
  }

  private val typeConverters: Map[(String, String), Tree => Tree] = Map(
    tcEntry[Double, BigDecimal](d => q"BigDecimal($d)"),
    tcEntry[Long, BigDecimal](l => q"BigDecimal($l)"),
    tcEntry[Long, Double](l => q"$l.toDouble"),
    tcEntry[Int, Long](i => q"$i.toLong"),
    tcEntry[Int, BigDecimal](i => q"BigDecimal($i)"),
    tcEntry[Short, BigDecimal](s => q"BigDecimal($s)"),
    tcEntry[Byte, BigDecimal](b => q"BigDecimal($b)")
  )

  private val truncTime = q"_root_.org.yupana.core.ExpressionCalculator.truncateTime"
  private val dtft = q"_root_.org.joda.time.DateTimeFieldType"

  private def exprValName(e: Expression[_]): TermName = {
    val suf = e.hashCode().toString.replace("-", "0")
    TermName(s"e_$suf")
  }

  private def ordValName(dt: DataType): TermName = {
    TermName(s"ord_${dt.toString}")
  }

  private def fractionalValName(dt: DataType): TermName = {
    TermName(s"frac_${dt.toString}")
  }

  private def integralValName(dt: DataType): TermName = {
    TermName(s"int_${dt.toString}")
  }

  private def mkLogical(
      row: TermName,
      state: State,
      e: Expression[_],
      cs: Seq[Condition],
      reducer: (Tree, Tree) => Tree
  ): State = {
    val newState = mkSetExprs(row, state, cs)
    val gets = cs.map(c => mkGet(newState, row, c))
    if (gets.nonEmpty)
      newState.withDefine(row, e, gets.reduceLeft(reducer))
    else newState
  }

  private def mkSet(
      row: TermName,
      state: State,
      e: Expression[_]
  ): State = {

    if (state.index.contains(e)) {
      state
    } else {

      e match {
        case ConstantExpr(c) =>
          val v = mapValue(e.dataType)(c)
          if (state.required.contains(e)) state.withDefine(row, e, v) else state

        case TimeExpr             => state.withExpr(e)
        case DimensionExpr(_)     => state.withExpr(e)
        case DimensionIdExpr(_)   => state.withExpr(e)
        case MetricExpr(_)        => state.withExpr(e)
        case DimIdInExpr(_, _)    => state.withExpr(e)
        case DimIdNotInExpr(_, _) => state.withExpr(e)
        case LinkExpr(link, _) =>
          val dimExpr = DimensionExpr(link.dimension)
          state.withExpr(dimExpr).withExpr(e)

        case _: AggregateExpr[_, _, _] => state.withRequired(e)
        case we: WindowFunctionExpr[_, _] =>
          val s1 = mkSet(row, state, TimeExpr)
          mkSet(row, s1, we.expr).withExpr(e)

        case TupleExpr(a, b) => mkSetBinary(row, state, e, a, b, (x, y) => q"($x, $y)")

        case GtExpr(a, b)  => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x > $y""")
        case LtExpr(a, b)  => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x < $y""")
        case GeExpr(a, b)  => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x >= $y""")
        case LeExpr(a, b)  => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x <= $y""")
        case EqExpr(a, b)  => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x == $y""")
        case NeqExpr(a, b) => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x != $y""")

        case InExpr(v, vs) =>
          mkSetUnary(row, state, e, v, x => q"""${exprValName(e)}.contains($x)""")
            .withGlobal(exprValName(e), tq"Set[${mkType(v)}]", mkSetValue(v, vs))
        case NotInExpr(v, vs) =>
          mkSetUnary(row, state, e, v, x => q"""!${exprValName(e)}.contains($x)""")
            .withGlobal(exprValName(e), tq"Set[${mkType(v)}]", mkSetValue(v, vs))

        case PlusExpr(a, b)    => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x + $y""")
        case MinusExpr(a, b)   => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x - $y""")
        case TimesExpr(a, b)   => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x * $y""")
        case DivIntExpr(a, b)  => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x / $y""")
        case DivFracExpr(a, b) => mkSetBinary(row, state, e, a, b, (x, y) => q"""$x / $y""")

        case TypeConvertExpr(_, a) => mkSetTypeConvertExpr(row, state, e, a)

        case TruncYearExpr(a) => mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.year())($x)""")
        case TruncMonthExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.monthOfYear())($x)""")
        case TruncWeekExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.weekOfWeekyear())($x)""")
        case TruncDayExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.dayOfMonth())($x)""")
        case TruncHourExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.hourOfDay())($x)""")
        case TruncMinuteExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.minuteOfHour())($x)""")
        case TruncSecondExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$truncTime($dtft.secondOfMinute())($x)""")
        case ExtractYearExpr(a) => mkSetUnary(row, state, e, a, x => q"$x.toLocalDateTime.getYear")
        case ExtractMonthExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"$x.toLocalDateTime.getMonthOfYear")
        case ExtractDayExpr(a)  => mkSetUnary(row, state, e, a, x => q"$x.toLocalDateTime.getDayOfMonth")
        case ExtractHourExpr(a) => mkSetUnary(row, state, e, a, x => q"$x.toLocalDateTime.getHourOfDay")
        case ExtractMinuteExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"$x.toLocalDateTime.getMinuteOfHour")
        case ExtractSecondExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"$x.toLocalDateTime.getSecondOfMinute")

        case TimeMinusExpr(a, b) =>
          mkSetBinary(row, state, e, a, b, (x, y) => q"_root_.scala.math.abs($x.millis - $y.millis)")
        case TimeMinusPeriodExpr(a, b) =>
          mkSetBinary(row, state, e, a, b, (t, p) => q"Time($t.toDateTime.minus($p).getMillis)")
        case TimePlusPeriodExpr(a, b) =>
          mkSetBinary(row, state, e, a, b, (t, p) => q"Time($t.toDateTime.plus($p).getMillis)")
        case PeriodPlusPeriodExpr(a, b) => mkSetBinary(row, state, e, a, b, (x, y) => q"$x plus $y")

        case IsNullExpr(a)    => mkSetUnary(row, state, e, a, _ => q"false", Some(q"true"))
        case IsNotNullExpr(a) => mkSetUnary(row, state, e, a, _ => q"true", Some(q"false"))

        case LowerExpr(a) => mkSetUnary(row, state, e, a, x => q"$x.toLowerCase")
        case UpperExpr(a) => mkSetUnary(row, state, e, a, x => q"$x.toUpperCase")

        case ConditionExpr(c, p, n) =>
          val newState = mkSetExprs(row, state, Seq(c, p, n))
          val getC = mkGet(newState, row, c)
          val getP = mkGet(newState, row, p)
          val getN = mkGet(newState, row, n)
          newState.withDefine(row, e, q"if ($getC) $getP else $getN")

        case AbsExpr(a)        => mkSetMathUnary(row, state, e, a, TermName("abs"))
        case UnaryMinusExpr(a) => mkSetMathUnary(row, state, e, a, TermName("negate"))

        case NotExpr(a)  => mkSetUnary(row, state, e, a, x => q"!$x")
        case AndExpr(cs) => mkLogical(row, state, e, cs, (a, b) => q"$a && $b")
        case OrExpr(cs)  => mkLogical(row, state, e, cs, (a, b) => q"$a || $b")

        case TokensExpr(a)    => mkSetUnary(row, state, e, a, x => q"$tokenizer.transliteratedTokens($x)")
        case SplitExpr(a)     => mkSetUnary(row, state, e, a, x => q"$calculator.splitBy($x, !_.isLetterOrDigit).toSeq")
        case LengthExpr(a)    => mkSetUnary(row, state, e, a, x => q"$x.length")
        case ConcatExpr(a, b) => mkSetBinary(row, state, e, a, b, (x, y) => q"$x + $y")

        case ArrayExpr(exprs) =>
          val newState = mkSetExprs(row, state, exprs)
          val gets = exprs.map(a => mkGet(newState, row, a))
          newState.withDefine(row, e, q"Seq(..$gets)")

        case ArrayLengthExpr(a)   => mkSetUnary(row, state, e, a, x => q"$x.size")
        case ArrayToStringExpr(a) => mkSetUnary(row, state, e, a, x => q"""$x.mkString(", ")""")
        case ArrayTokensExpr(a) =>
          mkSetUnary(row, state, e, a, x => q"""$x.flatMap(s => $tokenizer.transliteratedTokens(s))""")

        case ContainsExpr(as, b) => mkSetBinary(row, state, e, as, b, (x, y) => q"$x.contains($y)")
        case ContainsAnyExpr(as, bs) =>
          mkSetBinary(row, state, e, as, bs, (x, y) => q"$y.exists($x.contains)")
        case ContainsAllExpr(as, bs) =>
          mkSetBinary(row, state, e, as, bs, (x, y) => q"$y.forall($x.contains)")
        case ContainsSameExpr(as, bs) =>
          mkSetBinary(row, state, e, as, bs, (x, y) => q"$x.size == $y.size && $x.toSet == $y.toSet")
      }
    }
  }

  private def mkFilter(
      row: TermName,
      state: State,
      condition: Option[Expression.Condition]
  ): State = {
    condition match {
      case Some(ConstantExpr(v)) => state.appendLocal(q"$v")

      case Some(cond) =>
        val newState = mkSet(row, state, cond)
        val t = mkGet(newState, row, cond)
        newState.appendLocal(t)

      case None => state.appendLocal(q"true")
    }
  }

  private def mkSetExprs(
      row: TermName,
      state: State,
      exprs: Seq[Expression[_]]
  ): State = {
    exprs.foldLeft(state) {
      case (s, c) => mkSet(row, s, c)
    }
  }

  private def mkEvaluate(
      query: Query,
      row: TermName,
      state: State
  ): State = {
    mkSetExprs(row, state, query.fields.map(_.expr).toList ++ query.groupBy)
  }

  private def mkMap(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      row: TermName
  ): State = {
    val newState = mkSetExprs(row, state, aggregates.map(_.expr))

    aggregates.foldLeft(newState) { (s, ae) =>
      val exprValue = mkGet(newState, row, ae.expr)

      ae match {
        case SumExpr(_) => s.withDefine(row, ae, exprValue)
        case MinExpr(_) | MaxExpr(_) =>
          val aType = mkType(ae.expr)
          s.withDefine(row, ae, exprValue)
            .withGlobal(
              ordValName(ae.expr.dataType),
              tq"Ordering[${mkType(ae.expr)}]",
              q"DataType.bySqlName(${ae.expr.dataType.meta.sqlTypeName}).get.asInstanceOf[DataType.Aux[$aType]].ordering.get"
            )

        case CountExpr(_) =>
          mkIsDefined(s, row, ae.expr) match {
            case Some(d) => s.withDefine(row, ae, q"if ($d) 1L else 0L")
            case None    => s.withDefine(row, ae, q"1L")
          }

        case DistinctCountExpr(_) | DistinctRandomExpr(_) =>
          mkIsDefined(s, row, ae.expr) match {
            case Some(d) => s.withDefine(row, ae, q"if ($d) Set($exprValue) else Set.empty")
            case None    => s.withDefine(row, ae, q"Set($exprValue)")
          }

      }
    }
  }

  private def mkReduce(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      rowA: TermName,
      rowB: TermName,
      outRow: TermName
  ): Tree = {
    val trees = aggregates.map { ae =>
      val idx = state.index(ae)
      val valueTpe = mkType(ae.expr)

      val aValue = mkGet(state, rowA, ae)
      val bValue = mkGet(state, rowB, ae)

      val value = ae match {
        case SumExpr(_)            => q"$aValue + $bValue"
        case MinExpr(_)            => q"${ordValName(ae.expr.dataType)}.min($aValue, $bValue)"
        case MaxExpr(_)            => q"${ordValName(ae.expr.dataType)}.max($aValue, $bValue)"
        case CountExpr(_)          => q"$rowA.get[Long]($idx) + $rowB.get[Long]($idx)"
        case DistinctCountExpr(_)  => q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)"
        case DistinctRandomExpr(_) => q"$rowA.get[Set[$valueTpe]]($idx) ++ $rowB.get[Set[$valueTpe]]($idx)"
      }
      q"$outRow.set($idx, $value)"
    }

    q"..$trees"
  }

  private def mkPostMap(
      state: State,
      aggregates: Seq[AggregateExpr[_, _, _]],
      row: TermName
  ): Tree = {
    val trees = aggregates.flatMap { ae =>
      val idx = state.index(ae)
      val valueTpe = mkType(ae.expr)

      val oldValue = mkGet(state, row, ae)

      val value = ae match {
        case SumExpr(_)           => Some(q"if ($oldValue != null) $oldValue else 0")
        case MinExpr(_)           => None
        case MaxExpr(_)           => None
        case CountExpr(_)         => None
        case DistinctCountExpr(_) => Some(q"$row.get[Set[$valueTpe]]($idx).size")
        case DistinctRandomExpr(_) =>
          Some(
            q"""
              val s = $row.get[Set[$valueTpe]]($idx)
              val n = _root_.scala.util.Random.nextInt(s.size)
              s.iterator.drop(n).next
            """
          )
      }

      value.map(v => q"$row.set($idx, $v)")
    }

    q"..$trees"
  }

  private def mkPostAggregate(
      query: Query,
      row: TermName,
      state: State
  ): State = {
    mkSetExprs(row, state, query.fields.map(_.expr))
  }

  private def mkSetValue[T: TypeTag](inner: Expression[_], values: Set[T]): Tree = {
    val literals = values.toList.map(mapValue(inner.dataType))
    Apply(Ident(TermName("Set")), literals)
  }

  def generateCalculator(query: Query, condition: Option[Condition]): (Tree, Map[Expression[_], Int]) = {
    val internalRow = TermName("internalRow")
    val initialState =
      State(Map.empty, query.fields.map(_.expr).toSet ++ query.groupBy ++ condition, Map.empty, Seq.empty, Seq.empty)

    val (filter, filteredState) = mkFilter(internalRow, initialState, condition).fresh

    val (evaluate, evaluatedState) = mkEvaluate(query, internalRow, filteredState).fresh

    val knownAggregates = evaluatedState.index.collect { case (ae: AggregateExpr[_, _, _], _) => ae }.toSeq

    val s3 = evaluatedState //.copy(required = s2.required ++ knownAggregates)

    val (map, aggregateState) = mkMap(s3, knownAggregates, internalRow).fresh

    val rowA = TermName("rowA")
    val rowB = TermName("rowB")
    val outRow = rowA
    val reduce = mkReduce(aggregateState, knownAggregates, rowA, rowB, outRow)

    val postMap = mkPostMap(aggregateState, knownAggregates, internalRow)

    val (postAggregate, postAggregateState) = mkPostAggregate(query, internalRow, aggregateState).fresh

    val (postFilter, finalState) = mkFilter(internalRow, postAggregateState, query.postFilter).fresh
    val defs = q"""..${postAggregateState.globalDecls.values}"""

    val tree = q"""
        import _root_.org.yupana.api.Time
        import _root_.org.yupana.api.types.DataType
        import _root_.org.yupana.api.utils.Tokenizer
        import _root_.org.yupana.core.model.InternalRow
        
        new _root_.org.yupana.core.ExpressionCalculator {
          ..$defs
        
          override def evaluateFilter($tokenizer: Tokenizer, $internalRow: InternalRow): Boolean = $filter
          
          override def evaluateExpressions($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $evaluate
            $internalRow
          }
          
          override def evaluateMap($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $map
            $internalRow
          }
          
          override def evaluateReduce($tokenizer: Tokenizer, $rowA: InternalRow, $rowB: InternalRow): InternalRow = {
            $reduce
            $outRow
          }
    
          override def evaluatePostMap($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $postMap
            $internalRow
          }
    
          override def evaluatePostAggregateExprs($tokenizer: Tokenizer, $internalRow: InternalRow): InternalRow = {
            $postAggregate
            $internalRow
          }

          override def evaluatePostFilter($tokenizer: Tokenizer, $internalRow: InternalRow): Boolean = $postFilter
        }
    """

    tree -> finalState.index
  }

  def makeCalculator(query: Query, condition: Option[Condition]): (ExpressionCalculator, Map[Expression[_], Int]) = {
    val tb = currentMirror.mkToolBox()

    val (tree, known) = generateCalculator(query, condition)

    logger.whenTraceEnabled {
      val index = known.toList.sortBy(_._2).map { case (e, i) => s"$i -> $e" }
      logger.trace("Expr index: ")
      index.foreach(s => logger.trace(s"  $s"))
      logger.trace(s"Tree: ${prettyTree(tree)}")
    }

    (tb.compile(tree)().asInstanceOf[ExpressionCalculator], known)
  }

  private def prettyTree(tree: Tree): String = {
    show(tree)
      .replaceAll("_root_\\.([a-z_]+\\.)+", "")
      .replaceAll("\\.\\$bang\\$eq", " != ")
      .replaceAll("\\.\\$eq\\$eq", " == ")
      .replaceAll("\\.\\$amp\\$amp", " && ")
      .replaceAll("\\.\\$plus\\$plus", " ++ ")
      .replaceAll("\\.\\$plus", " + ")
      .replaceAll("\\.\\$minus", " - ")
      .replaceAll("\\.\\$div", " / ")
      .replaceAll("\\.\\$greater\\$eq", " >= ")
      .replaceAll("\\.\\$greater", " > ")
      .replaceAll("\\.\\$lower\\$eq", " <= ")
      .replaceAll("\\.\\$lower", " > ")
  }

  def truncateTime(fieldType: DateTimeFieldType)(time: Time): Time = {
    Time(time.toDateTime.property(fieldType).roundFloorCopy().getMillis)
  }

  def splitBy(s: String, p: Char => Boolean): Iterator[String] = new AbstractIterator[String] {
    private val len = s.length
    private var pos = 0

    override def hasNext: Boolean = pos < len

    override def next(): String = {
      if (pos >= len) throw new NoSuchElementException("next on empty iterator")
      val start = pos
      while (pos < len && !p(s(pos))) pos += 1
      val res = s.substring(start, pos min len)
      while (pos < len && p(s(pos))) pos += 1
      res
    }
  }
}
