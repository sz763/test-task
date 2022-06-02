package com.github.sz763

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, convertToAnyMustWrapper}

import java.util.UUID
import scala.collection.mutable.ListBuffer

class OffsetRepositoryTest extends AnyFunSuite {
  test("test save new offset") {
    val topicName = UUID.randomUUID().toString
    val repository = new OffsetRepository("jdbc:postgresql://localhost:5432/postgres", "sz",
      "1", "org.postgresql.Driver")
    repository.saveOffsetInfo(TopicOffsetInfo(topicName, ListBuffer(PartitionOffsetData("0", 10))))
    val topicOffsetInfo = repository.findOffsetInfo(topicName)
    val partData = topicOffsetInfo.partitionDataList.head
    partData.offset must be(10L)
  }

  test("test update existing offset") {
    val topicName = "test_topic"
    val repository = new OffsetRepository("jdbc:postgresql://localhost:5432/postgres", "sz",
      "1", "org.postgresql.Driver")
    val offset = System.currentTimeMillis()
    repository.saveOffsetInfo(TopicOffsetInfo(topicName, ListBuffer(PartitionOffsetData("0", 0L))))
    repository.saveOffsetInfo(TopicOffsetInfo(topicName, ListBuffer(PartitionOffsetData("0", offset))))
    val topicOffsetInfo = repository.findOffsetInfo(topicName)
    val partData = topicOffsetInfo.partitionDataList.head
    partData.offset must be(offset)
  }

}
