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

import org.yupana.api.schema.Dimension
import org.yupana.utils.Tokenizer

object ItemDimension {

  def apply(name: String): Dimension = {
    Dimension(name, Some(hash))
  }

  val stopWords: Set[String] = Set("kg", "ml", "lit", "litr", "gr", "sht")
  val numOfChars: Int = 4

  private val bs: List[String] = List("skdgnl", "pmfzrc", "tboi", "vaei", "jq", "uw", "x", "y")
  private val charIdx8: Map[Char, Int] = bs.zipWithIndex.flatMap { case (b, i) => b.map(c => (c, i)) }.toMap

  def hash(item: String): Int = {

    val stemmed = Tokenizer.transliteratedTokens(item).toArray

    val words = stemmed.filterNot(stopWords.contains).filter(i => i.length > 1 && i.forall(_.isLetter))

    (0 until numOfChars).foldLeft(0) { (h, pos) =>
      val code = if (pos == 0) {
        encode(chars(words, pos), charIdx8, 8)
      } else {
        math.abs(new String(chars(words, pos - 1).sorted).hashCode) % 255
      }
      (h << 8) | code
    }
  }

  private def encode(chars: Array[Char], charIdx: Map[Char, Int], nBits: Int) = {
    val bits = chars.map(c => charIdx(c) % nBits)
    bits.foldLeft(0) { (a, i) =>
      (1 << i) | a
    }
  }

  private def chars(words: Array[String], pos: Int) = {
    words
      .map { w =>
        if (w.length > pos) w(pos) else ' '
      }
      .filter(charIdx8.contains)
  }
}
