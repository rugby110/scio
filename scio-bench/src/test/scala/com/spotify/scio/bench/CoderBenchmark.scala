/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bench

import java.io.{InputStream, OutputStream}
import java.lang.{Integer => JInt}
import java.util.{Map => JMap}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.coders.{AtomicCoder, MapCoder, StringUtf8Coder, VarIntCoder}
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.coders.{KryoAtomicCoder, KryoRegistrator}
import com.twitter.chill._
import org.scalameter.api._

import scala.collection.JavaConverters._

case class Record(i: Int, s: String, m: Map[String, Int])

object CoderBenchmark extends Bench.LocalTime {

  val sizes = Gen.range("size")(10000, 50000, 10000)
  val inputs = for (s <- sizes)
    yield Array.fill(s)(Record(10, "hello", Map("one" -> 1, "two" -> 2, "three" -> 3)))

  val kryo = KryoAtomicCoder[Record]
  val customKryo = new KryoAtomicCoder[Record](new CustomKryoRegistrator)
  val native = new NativeAtomicCoder

  performance of "KryoAtomicCoder" in {
    measure method "roundTrip" in {
      using(inputs) in { xs =>
        var i = 0
        while (i < xs.length) {
          val bytes = CoderUtils.encodeToByteArray(kryo, xs(i))
          val copy = CoderUtils.decodeFromByteArray(kryo, bytes)
          i += 1
        }
      }
    }
  }

  performance of "CustomKryoAtomicCoder" in {
    measure method "roundTrip" in {
      using(inputs) in { xs =>
        var i = 0
        while (i < xs.length) {
          val bytes = CoderUtils.encodeToByteArray(customKryo, xs(i))
          val copy = CoderUtils.decodeFromByteArray(customKryo, bytes)
          i += 1
        }
      }
    }
  }

  performance of "NativeAtomicCoder" in {
    measure method "roundTrip" in {
      using(inputs) in { xs =>
        var i = 0
        while (i < xs.length) {
          val bytes = CoderUtils.encodeToByteArray(native, xs(i))
          val copy = CoderUtils.decodeFromByteArray(native, bytes)
          i += 1
        }
      }
    }
  }

  class RecordSerializer extends KSerializer[Record] {
    override def write(kser: Kryo, out: Output, obj: Record): Unit = {
      out.writeInt(obj.i)
      out.writeString(obj.s)
      kser.writeClassAndObject(out, obj.m)
    }
    override def read(kser: Kryo, in: Input, cls: Class[Record]): Record = {
      val i = in.readInt()
      val s = in.readString()
      val m = kser.readClassAndObject(in).asInstanceOf[Map[String, Int]]
      Record(i, s, m)
    }
  }
  class CustomKryoRegistrator extends KryoRegistrator {
    override def apply(k: Kryo): Unit = k.forClass[Record](new RecordSerializer)
  }

  class NativeAtomicCoder extends AtomicCoder[Record] {
    val intCoder = VarIntCoder.of()
    val strCoder = StringUtf8Coder.of()
    val mapCoder = MapCoder.of(strCoder, intCoder)

    override def encode(value: Record, outStream: OutputStream, context: Context): Unit = {
      intCoder.encode(value.i, outStream, Context.NESTED)
      strCoder.encode(value.s, outStream, Context.NESTED)
      mapCoder.encode(
        value.m.asJava.asInstanceOf[JMap[String, JInt]], outStream, Context.NESTED)
    }

    override def decode(inStream: InputStream, context: Context): Record = {
      val i = intCoder.decode(inStream, Context.NESTED)
      val s = strCoder.decode(inStream, Context.NESTED)
      val m = mapCoder.decode(inStream, Context.NESTED).asScala.toMap.asInstanceOf[Map[String, Int]]
      Record(i, s, m)
    }
  }

}
