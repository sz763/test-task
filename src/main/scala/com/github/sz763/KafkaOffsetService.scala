package com.github.sz763

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.{BufferHasAsJava, ListHasAsScala, MapHasAsScala}


class KafkaOffsetService(
                          val kafkaConsumer: KafkaConsumer[String, String], offsetRepository: OffsetRepository
                        ) extends LazyLogging {

  def getOffsets(topicName: String, batchSize: Int): (TopicOffsetInfo, TopicOffsetInfo) = {
    val _endOffsets = endOffsets(topicName)
    val _beginningOffsets = beginningOffsets(topicName)
    val startFrom = getStartingOffset(_endOffsets, _beginningOffsets)
    val endAt = getEndingOffset(startFrom, _endOffsets, batchSize)
    (startFrom, endAt)
  }

  def endOffsets(topic: String): TopicOffsetInfo = {
    getOffsetInfoFunc(topic, (k, v) => k.endOffsets(v.asJava))
  }

  def beginningOffsets(topic: String): TopicOffsetInfo = {
    getOffsetInfoFunc(topic, (k, v) => k.beginningOffsets(v.asJava))
  }


  private def getOffsetInfoFunc(topic: String,
                                offsetGetter: (KafkaConsumer[String, String],
                                  mutable.Buffer[TopicPartition]) => java.util.Map[TopicPartition, java.lang.Long]
                               ): TopicOffsetInfo = {
    val topicPartitions = kafkaConsumer.partitionsFor(topic).asScala
      .map(it => new TopicPartition(topic, it.partition()))
    val partitionInfo = offsetGetter.apply(kafkaConsumer, topicPartitions).asScala
    val topicOffsetInfo = TopicOffsetInfo(topic, ListBuffer[PartitionOffsetData]())
    partitionInfo.map { case (partition, offset) => PartitionOffsetData(partition.partition().toString, offset) }
      .foreach(it => topicOffsetInfo.partitionDataList.addOne(it))
    topicOffsetInfo
  }


  def getEndingOffset(startOffsetInfo: TopicOffsetInfo, topicOffsetInfo: TopicOffsetInfo, partitionBatchSize: Int): TopicOffsetInfo = {

    val partitionInfoToRead = TopicOffsetInfo(startOffsetInfo.topic, ListBuffer[PartitionOffsetData]())

    val maxToReadOffsetInfoMap = startOffsetInfo
      .partitionDataList
      .map(partitionOffsetData => PartitionOffsetData(partitionOffsetData.partitionIndex, partitionOffsetData.offset + partitionBatchSize))
      .map(partitionOffsetData => (partitionOffsetData.partitionIndex, partitionOffsetData.offset))
      .toMap

    topicOffsetInfo
      .partitionDataList
      .map(partitionOffsetData => {
        val partition = partitionOffsetData.partitionIndex
        PartitionOffsetData(partition, Math.min(partitionOffsetData.offset, maxToReadOffsetInfoMap(partition)))
      })
      .foreach(partitionOffsetData => partitionInfoToRead.partitionDataList.addOne(partitionOffsetData))

    partitionInfoToRead
  }

  def getStartingOffset(endingOffset: TopicOffsetInfo, beginningOffset: TopicOffsetInfo): TopicOffsetInfo = {
    val startingOffsetQuery = offsetRepository.findOffsetInfo(endingOffset.topic)

    if (startingOffsetQuery != null) {
      return startingOffsetQuery
    }

    val zeroOffsetPartitions = TopicOffsetInfo(endingOffset.topic, ListBuffer[PartitionOffsetData]())

    val earliestOffsets = beginningOffset
      .partitionDataList
      .map(offsetInfo => (offsetInfo.partitionIndex, offsetInfo.offset))
      .toMap

    endingOffset.partitionDataList.foreach((offsetInfo: PartitionOffsetData) => {
      PartitionOffsetData(offsetInfo.partitionIndex, offsetInfo.offset)
      zeroOffsetPartitions.partitionDataList.addOne(PartitionOffsetData(offsetInfo.partitionIndex, earliestOffsets(offsetInfo.partitionIndex)))
    })
    zeroOffsetPartitions
  }


}

case class TopicOffsetInfo(topic: String, partitionDataList: ListBuffer[PartitionOffsetData]) {
  override def toString: String = {
    val offsets = partitionDataList.map(it => s""""${it.partitionIndex}":${it.offset}""").mkString(",")
    val value = s"""{"$topic":{$offsets}}"""
    value
  }
}

case class PartitionOffsetData(partitionIndex: String, offset: Long)