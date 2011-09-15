package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import com.twitter.cassie.codecs.IntCodec


class IntCodecTest extends CodecTest {
  describe("encoding an int") {
    it("produces a variable length zig-zag encoded array of bytes") {
      IntCodec.encode(199181) must equal(bb(0, 3, 10, 13))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an int") {
      IntCodec.decode(bb(0, 3, 10, 13)) must equal(199181)
    }
  }

  check { i: Int => IntCodec.decode(IntCodec.encode(i)) == i }
}
