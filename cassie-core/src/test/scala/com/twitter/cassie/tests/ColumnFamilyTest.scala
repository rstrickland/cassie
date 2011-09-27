package com.twitter.cassie.tests

import scala.collection.JavaConversions._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie.codecs.Utf8Codec
import org.mockito.Mockito.{when, verify}
import org.mockito.Matchers.{any, eq => matchEq, anyListOf}
import org.apache.cassandra.finagle.thrift
import org.mockito.ArgumentCaptor
import java.nio.ByteBuffer
import com.twitter.cassie._
import scala.collection.mutable.ListBuffer
import com.twitter.cassie.util.ColumnFamilyTestHelper
import com.twitter.util.Future
import java.util.{ArrayList => JArrayList}

class ColumnFamilyTest extends Spec with MustMatchers with MockitoSugar with ColumnFamilyTestHelper {

  describe("page through columns") {
    val (client, cf) = setup

    it("can return exactly one column") {
      val key = "trance"
      val cp = new thrift.ColumnParent("cf")

      val columns1 = Seq(c(cf, "dj", "Armin van Buuren", 2293L))

      val range1 = new thrift.SliceRange(b(""), b(""), false, 2)
      val pred1 = new thrift.SlicePredicate()
      pred1.setSlice_range(range1)
      when(client.get_slice(b(key), cp, pred1, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns1))

      val l = new ListBuffer[String]
      cf.columnsIteratee(2, key).foreach { c => l.append(c.name) }
      l must equal(List("dj"))
    }

    it("fetches multiple slices") {
      val key = "trance"
      val cp = new thrift.ColumnParent("cf")

      val columns1 = Seq(c(cf, "cat", "Commie", 2293L), c(cf, "name", "Coda", 2292L))
      val columns2 = Seq(c(cf, "name", "Coda", 2292L), c(cf, "radish", "red", 2294L), c(cf, "sofa", "plush", 2298L))
      val columns3 = Seq(c(cf, "sofa", "plush", 2298L), c(cf, "xray", "ow", 2294L))

      val range1 = new thrift.SliceRange(b(""), b(""), false, 2)
      val pred1 = new thrift.SlicePredicate()
      pred1.setSlice_range(range1)
      when(client.get_slice(b(key), cp, pred1, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns1))

      val range2 = new thrift.SliceRange(b("name"), b(""), false, 3)
      val pred2 = new thrift.SlicePredicate()
      pred2.setSlice_range(range2)
      when(client.get_slice(b(key), cp, pred2, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns2))

      val range3 = new thrift.SliceRange(b("sofa"), b(""), false, 3)
      val pred3 = new thrift.SlicePredicate()
      pred3.setSlice_range(range3)
      when(client.get_slice(b(key), cp, pred3, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns3))

      val l = new ListBuffer[String]
      cf.columnsIteratee(2, key).foreach { c => l.append(c.name) }
      l must equal(List("cat", "name", "radish", "sofa", "xray"))
    }
  }

  describe("getting a columns for a key") {
    val (client, cf) = setup

    it("performs a get_slice with a set of column names") {
      cf.getColumn("key", "name")

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns none if the column doesn't exist") {
      when(client.get_slice(anyByteBuffer(), anyColumnParent(), anySlicePredicate(),
          anyConsistencyLevel()))
          .thenReturn(Future.value(new JArrayList[thrift.ColumnOrSuperColumn]()))

      cf.getColumn("key", "name")() must equal(None)
    }

    it("returns a option of a column if it exists") {
      val columns = Seq(c(cf, "name", "Coda", 2292L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[ColumnList](columns))

      cf.getColumn("key", "name")() must equal(Some(Column("name", "Coda").timestamp(2292L)))
    }
  }

  describe("getting a row") {
    val (client, cf) = setup

    it("performs a get_slice with a maxed-out count") {
      val columns = Seq(c(cf, "name", "Coda", 2292L),
                        c(cf, "age", "old", 11919L))
      val cp = new thrift.ColumnParent("cf")
      val range = new thrift.SliceRange(b(""), b(""), false, Int.MaxValue)
      val pred = new thrift.SlicePredicate()
      pred.setSlice_range(range)

      when(client.get_slice(b("key"), cp, pred, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns))

      cf.getRow("key")
    }

    it("returns a map of columns") {
      val columns = Seq(c(cf, "name", "Coda", 2292L),
                        c(cf, "age", "old", 11919L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[ColumnList](columns))

      cf.getRow("key")() must equal(asJavaMap(Map(
        "name" -> Column("name", "Coda").timestamp(2292L),
        "age" -> Column("age", "old").timestamp(11919L)
      )))
    }
  }

  describe("getting a row slice") {
    val (client, cf) = setup

    it("performs a get_slice with the specified count, reversedness, start column name and end column name") {
      val startColumnName = "somewhere"
      val endColumnName   = "overTheRainbow"
      cf.getRowSlice("key", Some(startColumnName), Some(endColumnName), 100, Order.Reversed)

      val cp = new thrift.ColumnParent("cf")

      val range = new thrift.SliceRange(Utf8Codec.encode(startColumnName), Utf8Codec.encode(endColumnName), true, 100)
      val pred  = new thrift.SlicePredicate()
      pred.setSlice_range(range)

      verify(client).get_slice(b("key"), cp, pred, thrift.ConsistencyLevel.QUORUM)
    }
  }

  describe("getting a set of columns for a key") {
    val (client, cf) = setup

    it("performs a get_slice with a set of column names") {
      cf.getColumns("key", Set("name", "age"))

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name", "age"))
    }

    it("returns a map of column names to columns") {
      val columns = Seq(c(cf, "name", "Coda", 2292L),
                        c(cf, "age", "old", 11919L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[ColumnList](columns))

      cf.getColumns("key", Set("name", "age"))() must equal(asJavaMap(Map(
        "name" -> Column("name", "Coda").timestamp(2292L),
        "age" -> Column("age", "old").timestamp(11919L)
      )))
    }
  }

  describe("getting a column for a set of keys") {
    val (client, cf) = setup

    it("performs a multiget_slice with a column name") {
      cf.consistency(ReadConsistency.One).multigetColumn(Set("key1", "key2"), "name")

      val keys = List("key1", "key2").map{Utf8Codec.encode(_)}
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("key1") -> asJavaList(Seq(c(cf, "name", "Coda", 2292L))),
        b("key2") -> asJavaList(Seq(c(cf, "name", "Niki", 422L)))
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[KeyColumnMap](results))

      cf.multigetColumn(Set("key1", "key2"), "name")() must equal(asJavaMap(Map(
        "key1" -> Column("name", "Coda").timestamp(2292L),
        "key2" -> Column("name", "Niki").timestamp(422L)
      )))
    }

    it("does not explode when the column doesn't exist for a key") {
      val results = Map(
        b("key1") -> asJavaList(Seq(c(cf, "name", "Coda", 2292L))),
        b("key2") -> (asJavaList(Seq()): ColumnList)
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[KeyColumnMap](results))

      cf.multigetColumn(Set("key1", "key2"), "name")() must equal(asJavaMap(Map(
        "key1" -> Column("name", "Coda").timestamp(2292L)
      )))
    }
  }

  describe("getting a set of columns for a set of keys") {
    val (client, cf) = setup

    it("performs a multiget_slice with a set of column names") {
      cf.consistency(ReadConsistency.One).multigetColumns(Set("key1", "key2"), Set("name", "age"))

      val keys = List("key1", "key2").map{ b(_) }
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name", "age"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("key1") -> asJavaList(Seq(c(cf, "name", "Coda", 2292L),
                                c(cf, "age", "old", 11919L))),
        b("key2") -> asJavaList(Seq(c(cf, "name", "Niki", 422L),
                                c(cf, "age", "lithe", 129L)))
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[KeyColumnMap](results))

      cf.multigetColumns(Set("key1", "key2"), Set("name", "age"))() must equal(asJavaMap(Map(
        "key1" -> asJavaMap(Map(
          "name" -> Column("name", "Coda").timestamp(2292L),
          "age" -> Column("age", "old").timestamp(11919L)
        )),
        "key2" -> asJavaMap(Map(
          "name" -> Column("name", "Niki").timestamp(422L),
          "age" -> Column("age", "lithe").timestamp(129L)
        ))
      )))
    }
  }

  describe("inserting a column") {
    val (client, cf) = setup

    it("performs an insert") {
      cf.insert("key", Column("name", "Coda").timestamp(55))

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnParent])
      val col = c(cf, "name", "Coda", 55).column

      verify(client).insert(matchEq(b("key")), cp.capture, matchEq(col), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
    }
  }

  describe("removing a row with a specific timestamp") {
    val (client, cf) = setup

    it("performs a remove") {
      when(client.remove(anyByteBuffer(), anyColumnPath(), anyInt(), anyConsistencyLevel()))
        .thenReturn(Future.void)

      cf.removeRowWithTimestamp("key", 55)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq(b("key")), cp.capture, matchEq(55L), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.column_family must equal("cf")
      cp.getValue.column must be(null)
    }
  }

  describe("performing a batch mutation") {
    val (client, cf) = setup

    it("performs a batch_mutate") {
      cf.consistency(WriteConsistency.All).batch()
        .insert("key", Column("name", "value").timestamp(201))
        .execute()

      val map = ArgumentCaptor.forClass(classOf[java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[thrift.Mutation]]]])

      verify(client).batch_mutate(map.capture, matchEq(thrift.ConsistencyLevel.ALL))

      val mutations = map.getValue
      val mutation = mutations.get(b("key")).get("cf").get(0)
      val col = mutation.getColumn_or_supercolumn.getColumn
      Utf8Codec.decode(col.name) must equal("name")
      Utf8Codec.decode(col.value) must equal("value")
      col.getTimestamp must equal(201)
    }
  }

  describe("exception handling") {
    // TODO
    // getRowSlice
    // getColumns
    // multigetColumns
    // insert
    // removeColumn
    // rowsIteratee
  }
}

