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

package org.yupana.schema.externallinks

import org.yupana.api.schema.{ Dimension, ExternalLink, LinkField }
import org.yupana.schema.Dimensions

trait RelatedItemsCatalog extends ExternalLink {
  val ITEM_FIELD = "item"
  val PHRASE_FIELD = "phrase"
  override type DimType = String

  override val linkName: String = "RelatedItemsCatalog"
  override val dimension: Dimension.Aux[String] = Dimensions.ITEM
  override val fields: Set[LinkField] = Set(
    LinkField[String](ITEM_FIELD),
    LinkField[String](PHRASE_FIELD)
  )
}

object RelatedItemsCatalog extends RelatedItemsCatalog
