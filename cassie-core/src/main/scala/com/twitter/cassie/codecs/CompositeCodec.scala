package com.twitter.cassie.codecs

import com.twitter.cassie.types._
import java.nio.ByteBuffer

object CompositeCodec {
  // convenience methods to make the creation syntax more concise
  def apply[T1](                c1: Codec[T1]): CompositeCodec[Composite1[T1]] = CompositeCodec(Composite(c1))

  def apply[T1, T2](            c1: Codec[T1],
                                c2: Codec[T2]): CompositeCodec[Composite2[T1, T2]] = CompositeCodec(Composite(c1, c2))

  def apply[T1, T2, T3](        c1: Codec[T1],
                                c2: Codec[T2],
                                c3: Codec[T3]): CompositeCodec[Composite3[T1, T2, T3]] = CompositeCodec(Composite(c1, c2, c3))

  def apply[T1, T2, T3, T4](    c1: Codec[T1],
                                c2: Codec[T2],
                                c3: Codec[T3],
                                c4: Codec[T4]): CompositeCodec[Composite4[T1, T2, T3, T4]] = CompositeCodec(Composite(c1, c2, c3, c4))

  def apply[T1, T2, T3, T4, T5](c1: Codec[T1],
                                c2: Codec[T2],
                                c3: Codec[T3],
                                c4: Codec[T4],
                                c5: Codec[T5]): CompositeCodec[Composite5[T1, T2, T3, T4, T5]] = CompositeCodec(Composite(c1, c2, c3, c4, c5))
}

case class CompositeCodec[A <: Composite](composite: A) extends Codec[A] {
  def encode(c: A): ByteBuffer = c.encode
  def decode(buf: ByteBuffer): A = composite.decode(buf).asInstanceOf[A]
}
