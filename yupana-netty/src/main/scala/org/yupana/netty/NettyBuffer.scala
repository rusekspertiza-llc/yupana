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

package org.yupana.netty

import io.netty.buffer.{ ByteBuf, Unpooled }
import org.yupana.protocol.Buffer

object NettyBuffer {
  implicit val nettyBuffer: Buffer[ByteBuf] = new Buffer[ByteBuf] {

    override def alloc(): ByteBuf = Unpooled.buffer()
    override def alloc(capacity: Int): ByteBuf = Unpooled.buffer(capacity)
    override def wrap(data: Array[Byte]): ByteBuf = Unpooled.wrappedBuffer(data)
    override def toByteArray(b: ByteBuf): Array[Byte] = b.array()

    override def readByte(b: ByteBuf): Byte = b.readByte()
    override def writeByte(b: ByteBuf, v: Byte): ByteBuf = b.writeByte(v)

    override def readInt(b: ByteBuf): Int = b.readInt()
    override def writeInt(b: ByteBuf, i: Int): ByteBuf = b.writeInt(i)

    override def readLong(b: ByteBuf): Long = b.readLong()
    override def writeLong(b: ByteBuf, l: Long): ByteBuf = b.writeLong(l)

    override def readDouble(b: ByteBuf): Double = b.readDouble()
    override def writeDouble(b: ByteBuf, d: Double): ByteBuf = b.writeDouble(d)

    override def read(b: ByteBuf, dst: Array[Byte]): Unit = b.readBytes(dst)
    override def write(b: ByteBuf, bytes: Array[Byte]): ByteBuf = b.writeBytes(bytes)
  }
}