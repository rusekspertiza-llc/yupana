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

package org.yupana.core.jit.codegen.expressions.regular

import org.yupana.api.query.NowExpr
import org.yupana.core.jit.{ CodeGenResult, JIT, State }
import org.yupana.core.jit.codegen.expressions.ExpressionCodeGen

import scala.reflect.runtime.universe._

object NowExpressionCodeGen extends ExpressionCodeGen[NowExpr.type] {
  override def expression: NowExpr.type = NowExpr

  override def generateEvalCode(state: State, row: TermName): CodeGenResult = {
    val (valueDeclaration, exprState) = state.withLocalValueDeclaration(expression)
    val validityTree = q"val ${valueDeclaration.validityFlagName} = true"
    val valueTree = q"val ${valueDeclaration.valueName} = ${JIT.NOW}"
    CodeGenResult(Seq(validityTree, valueTree), valueDeclaration, exprState)
  }
}
