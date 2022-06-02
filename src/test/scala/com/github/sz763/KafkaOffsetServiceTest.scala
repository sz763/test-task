package com.github.sz763

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{convertToAnyMustWrapper, equal}

import scala.jdk.CollectionConverters.MapHasAsJava

class KafkaOffsetServiceTest extends AnyFunSuite {

  test("test get end offset", Tag("integration")) {
    val topicName = "test_get_end_offset"
    val consumer = new KafkaConsumer[String, String](consumerConfig().asJava)
    val producer = new KafkaProducer[String, String](producerConfig().asJava)
    val service = new KafkaOffsetService(consumer, null)
    val offsetInfo = service.endOffsets(topicName)
    val offset = offsetInfo.partitionDataList.head.offset
    producer.send(new ProducerRecord[String, String](topicName, "key", "value")).get()
    val changedOffset = service.endOffsets(topicName).partitionDataList.head.offset
    changedOffset must equal(offset + 1)
  }

  private def consumerConfig(): Map[String, Object] = {
    val config: Map[String, Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.CLIENT_ID_CONFIG -> "test",
      ConsumerConfig.GROUP_ID_CONFIG -> "test-app-group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
    )
    config
  }

  private def producerConfig(): Map[String, Object] = {
    val config: Map[String, Object] = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ProducerConfig.CLIENT_ID_CONFIG -> "test",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    )
    config
  }
}
