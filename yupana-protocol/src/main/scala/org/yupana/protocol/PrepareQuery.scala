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

package org.yupana.protocol

case class PrepareQuery(id: Int, query: String, params: Map[Int, ParameterValue])
    extends Command[PrepareQuery](PrepareQuery)

object PrepareQuery extends MessageHelper[PrepareQuery] {
  override val tag: Byte = Tags.PREPARE_QUERY
  override val readWrite: ReadWrite[PrepareQuery] =
    ReadWrite.product3[PrepareQuery, Int, String, Map[Int, ParameterValue]](PrepareQuery.apply)(q =>
      (q.id, q.query, q.params)
    )
}
