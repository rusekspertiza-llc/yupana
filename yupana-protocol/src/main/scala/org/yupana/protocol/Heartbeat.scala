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

/** Keep alive message */
case class Heartbeat(time: Int) extends Response[Heartbeat](Heartbeat)

object Heartbeat extends MessageHelper[Heartbeat] {
  override val tag: Byte = Tags.HEARTBEAT
  override val readWrite: ReadWrite[Heartbeat] = implicitly[ReadWrite[Int]].imap(Heartbeat.apply)(_.time)
}
