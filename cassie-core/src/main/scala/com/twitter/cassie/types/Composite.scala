package com.twitter.cassie.types

import java.nio.ByteBuffer
import scala.collection.mutable
import com.twitter.cassie.codecs.Codec

object Composite {
  // convenience methods for creating composites
  def apply[T1](                c1: Component[T1]) = Composite1(c1)

  def apply[T1, T2](            c1: Component[T1],
                                c2: Component[T2]) = Composite2(c1, c2)

  def apply[T1, T2, T3](        c1: Component[T1],
                                c2: Component[T2],
                                c3: Component[T3]) = Composite3(c1, c2, c3)

  def apply[T1, T2, T3, T4](    c1: Component[T1],
                                c2: Component[T2],
                                c3: Component[T3],
                                c4: Component[T4]) = Composite4(c1, c2, c3, c4)

  def apply[T1, T2, T3, T4, T5](c1: Component[T1],
                                c2: Component[T2],
                                c3: Component[T3],
                                c4: Component[T4],
                                c5: Component[T5]) = Composite5(c1, c2, c3, c4, c5)

  // convenience methods for creating empty composites (for instantiating the CompositeCodec)
  def apply[T1](                c1: Codec[T1]) = Composite1(Component(c1))

  def apply[T1, T2](            c1: Codec[T1],
                                c2: Codec[T2]) = Composite2(Component(c1),
                                                            Component(c2))

  def apply[T1, T2, T3](        c1: Codec[T1],
                                c2: Codec[T2],
                                c3: Codec[T3]) = Composite3(Component(c1),
                                                            Component(c2),
                                                            Component(c3))

  def apply[T1, T2, T3, T4](    c1: Codec[T1],
                                c2: Codec[T2],
                                c3: Codec[T3],
                                c4: Codec[T4]) = Composite4(Component(c1),
                                                            Component(c2),
                                                            Component(c3),
                                                            Component(c4))

  def apply[T1, T2, T3, T4, T5](c1: Codec[T1],
                                c2: Codec[T2],
                                c3: Codec[T3],
                                c4: Codec[T4],
                                c5: Codec[T5]) = Composite5(Component(c1),
                                                            Component(c2),
                                                            Component(c3),
                                                            Component(c4),
                                                            Component(c5))
}

trait Composite {

  def arity: Int
  def element(n: Int): Component[_]
  def add[A](component: Component[A]): Composite
  def decode(encoded: ByteBuffer): Composite

  def encode: ByteBuffer = {
    val encodedComponents = iterator.map(c => (c.encode, c.equality)).toSeq
    //each component has 2-byte length + value + terminator
    val totalLength = encodedComponents.foldLeft(0) { (acc, c) => acc + 2 + c._1.remaining + 1 }
    val encoded = ByteBuffer.allocate(totalLength)

    encodedComponents.foreach { case (component, equality) =>
      val length = component.remaining
      // add length
      encoded.putShort(length.toShort)
      // add value
      encoded.put(component)
      // add terminator
      encoded.put(equality)
    }

    encoded.rewind
    encoded
  }

  // get the raw composite data out of the ByteBuffer
  def extract(encoded: ByteBuffer, acc: Seq[(ByteBuffer, ComponentEquality.ComponentEquality)] = Seq()):
  Seq[(ByteBuffer, ComponentEquality.ComponentEquality)] = {
    def extractComponent(buf: ByteBuffer) = {
      val length = buf.getShort
      val value = new Array[Byte](length)
      buf.get(value)
      (ByteBuffer.wrap(value), ComponentEquality.fromByte(buf.get))
    }
    if (encoded.remaining == 0) acc
    else extract(encoded, extractComponent(encoded) +: acc)
  }

  def validateLength(encodedLength: Int) {
    if (encodedLength != arity) throw new Exception("Attempted to create Composite%d from %d components".format(arity, encodedLength))
  }

  def iterator: Iterator[Component[_]] = new Iterator[Component[_]] {
    private var c: Int = 0
    private val cmax = arity
    def hasNext = c < cmax
    def next() = { val result = element(c); c += 1; result }
  }

  protected def buildComponent[A](component: Component[A],
                                  encoded: (ByteBuffer, ComponentEquality.ComponentEquality)) =
    Component(component.codec, component.decode(encoded._1), encoded._2)

}

/*
 * This tuple-style implementation is required in order to decode without the client having
 * to manually cast or provide type information at access time, thanks to erasure.
 */

case class Composite1[T1](_1: Component[T1]) extends Composite {

  override def arity = 1

  override def element(n: Int): Component[_] =  n match {
    case 0 => _1
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def add[A](component: Component[A]) = Composite2(_1, component)

  override def decode(encoded: ByteBuffer): Composite = {
    val enc = extract(encoded)
    validateLength(enc.length)
    Composite1(buildComponent(_1, enc(0)))
  }

}

case class Composite2[T1, T2](_1: Component[T1],
                              _2: Component[T2]) extends Composite {

  override def arity = 2

  override def element(n: Int): Component[_] =  n match {
    case 0 => _1
    case 1 => _2
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def add[A](component: Component[A]) = Composite3(_1, _2, component)

  override def decode(encoded: ByteBuffer): Composite = {
    val enc = extract(encoded)
    validateLength(enc.length)
    Composite2(buildComponent(_1, enc(0)),
               buildComponent(_2, enc(1)))
  }

}

case class Composite3[T1, T2, T3](_1: Component[T1],
                                  _2: Component[T2],
                                  _3: Component[T3]) extends Composite {

  override def arity = 3

  override def element(n: Int): Component[_] =  n match {
    case 0 => _1
    case 1 => _2
    case 2 => _3
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def add[A](component: Component[A]) = Composite4(_1, _2, _3, component)

  override def decode(encoded: ByteBuffer): Composite = {
    val enc = extract(encoded)
    validateLength(enc.length)
    Composite3(buildComponent(_1, enc(0)),
               buildComponent(_2, enc(1)),
               buildComponent(_3, enc(2)))
  }

}

case class Composite4[T1, T2, T3, T4](_1: Component[T1],
                                      _2: Component[T2],
                                      _3: Component[T3],
                                      _4: Component[T4]) extends Composite {

  override def arity = 4

  override def element(n: Int): Component[_] =  n match {
    case 0 => _1
    case 1 => _2
    case 2 => _3
    case 3 => _4
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def add[A](component: Component[A]) = Composite5(_1, _2, _3, _4, component)

  override def decode(encoded: ByteBuffer): Composite = {
    val enc = extract(encoded)
    validateLength(enc.length)
    Composite4(buildComponent(_1, enc(0)),
               buildComponent(_2, enc(1)),
               buildComponent(_3, enc(2)),
               buildComponent(_4, enc(3)))
  }

}

case class Composite5[T1, T2, T3, T4, T5](_1: Component[T1],
                                          _2: Component[T2],
                                          _3: Component[T3],
                                          _4: Component[T4],
                                          _5: Component[T5]) extends Composite {

  override def arity = 5

  override def element(n: Int): Component[_] =  n match {
    case 0 => _1
    case 1 => _2
    case 2 => _3
    case 3 => _4
    case 4 => _5
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def add[A](component: Component[A]) = throw new UnsupportedOperationException("Cannot add to Composite5")

  override def decode(encoded: ByteBuffer): Composite = {
    val enc = extract(encoded)
    validateLength(enc.length)
    Composite5(buildComponent(_1, enc(0)),
               buildComponent(_2, enc(1)),
               buildComponent(_3, enc(2)),
               buildComponent(_4, enc(3)),
               buildComponent(_5, enc(4)))
  }

}

object Component {
  def apply[A](codec: Codec[A]): Component[A] = Component(codec, None, ComponentEquality.EQ)

  def apply[A](codec: Codec[A], value: A, equality: ComponentEquality.ComponentEquality = ComponentEquality.EQ): Component[A] =
    Component(codec, Some(value), equality)
}

case class Component[A](codec: Codec[A], value: Option[A], equality: ComponentEquality.ComponentEquality) {
  def encode = value match {
    case Some(v) => codec.encode(v)
    case None => throw new UnsupportedOperationException("Cannot encode empty component")
  }
  def decode(buf: ByteBuffer): A = codec.decode(buf)
}

object ComponentEquality {

  type ComponentEquality = Byte
  val EQ:Byte = 0x0
  val LTE:Byte = -0x1
  val GTE:Byte = 0x1

  def fromByte(b: Byte) : ComponentEquality = {
    if (b > 0) GTE
    else if (b < 0) LTE
    else EQ
  }

}
