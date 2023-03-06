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

/**
  * Simple writer monad implementation.  For simplicity we assume that the log is always a List, and run the
  * identity monad.
  *
  * @tparam L log type
  * @tparam V value type
  */
final case class Writer[L, +V](run: (List[L], V)) {

  def tell(l: L): Writer[L, V] = Writer((run._1 :+ l, run._2))

  def flatMap[U](f: V => Writer[L, U]): Writer[L, U] = Writer {
    val (l, v) = f(run._2).run
    run._1 ++ l -> v
  }

  def tap[U](f: V => U): Writer[L, V] = {
    f(run._2)
    this
  }

  def map[U](f: V => U): Writer[L, U] = Writer(run._1 -> f(run._2))

  def traverse[U](f: V => Iterable[Writer[L, U]]): Writer[L, Iterable[U]] = {
    val writers = f(run._2)
    val log = run._1 ++ writers.flatMap(_.run._1)
    Writer(log -> writers.map(_.run._2))
  }

  def traverseIterator[U](f: V => IterableOnce[Writer[L, U]]): Writer[L, Iterator[U]] = {
    val writers = f(run._2)
    val log = run._1 ++ writers.iterator.flatMap(_.run._1)
    Writer(log -> writers.iterator.map(_.run._2))
  }
}

object Writer {
  def apply[L, V](init: V): Writer[L, V] = Writer(Nil -> init)

  def traverseList[L, T, U](list: List[T])(f: T => Writer[L, U]): Writer[L, List[U]] = {
    val (l, ru) = list.foldLeft((List.empty[L], List.empty[U])) {
      case ((ls, us), x) =>
        val (l, u) = f(x).run
        (ls ++ l, u :: us)
    }
    Writer(l -> ru.reverse)
  }

  def flatTraverseList[L, T, U](list: List[T])(f: T => Writer[L, List[U]]): Writer[L, List[U]] = {
    Writer(list.foldLeft((List.empty[L], List.empty[U])) {
      case ((ls, us), x) =>
        val (l, u) = f(x).run
        (ls ++ l, us ++ u)
    })
  }

  def flatTraverseIterator[L, T, U](list: Iterator[T])(f: T => Writer[L, IterableOnce[U]]): Writer[L, Iterator[U]] = {
    Writer(list.foldLeft((List.empty[L], Iterator.empty[U])) {
      case ((ls, us), x) =>
        val (l, u) = f(x).run
        (ls ++ l, us ++ u)
    })
  }
}
