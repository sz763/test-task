package com.github.sz763

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{convertToAnyMustWrapper, equal}

import scala.collection.mutable.ListBuffer

class TopicOffsetInfoTest extends AnyFunSuite {
  test("test building topic offset") {
    val offsetInfo = TopicOffsetInfo("test", ListBuffer(PartitionOffsetData("0", 1), PartitionOffsetData("1", 2)))
    offsetInfo.toString must equal("{\"test\":{\"0\":1,\"1\":2}}")
  }
}
