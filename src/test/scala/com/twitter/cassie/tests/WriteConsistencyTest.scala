package com.twitter.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.WriteConsistency
import org.apache.cassandra.finagle.thrift.ConsistencyLevel

class WriteConsistencyTest extends Spec with MustMatchers {
  describe("a write consistency of Any") {
    it("is human readable") {
      WriteConsistency.Any.toString must equal("WriteConsistency.Any")
    }

    it("has a corresponding Thrift ConsistencyLevel of ANY") {
      WriteConsistency.Any.level must equal(ConsistencyLevel.ANY)
    }
  }

  describe("a write consistency of One") {
    it("is human readable") {
      WriteConsistency.One.toString must equal("WriteConsistency.One")
    }

    it("has a corresponding Thrift ConsistencyLevel of ONE") {
      WriteConsistency.One.level must equal(ConsistencyLevel.ONE)
    }
  }

  describe("a write consistency of Quorum") {
    it("is human readable") {
      WriteConsistency.Quorum.toString must equal("WriteConsistency.Quorum")
    }

    it("has a corresponding Thrift ConsistencyLevel of QUORUM") {
      WriteConsistency.Quorum.level must equal(ConsistencyLevel.QUORUM)
    }
  }

  describe("a write consistency of All") {
    it("is human readable") {
      WriteConsistency.All.toString must equal("WriteConsistency.All")
    }

    it("has a corresponding Thrift ConsistencyLevel of ALL") {
      WriteConsistency.All.level must equal(ConsistencyLevel.ALL)
    }
  }
}
