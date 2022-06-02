package com.github.sz763

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters.MapHasAsJava

class KafkaLoaderTest extends AnyFunSuite with TestSparkSession {
  test("load data from kafka", Tag("integration")) {
    val topicName = "test_load_json"
    val producer = new KafkaProducer[String, String](producerConfig().asJava)
    val config = consumerConfig()
    val consumer = new KafkaConsumer[String, String](config.asJava)
    val jsonString = """{"id":1, "timestamp":"2022-01-01 20:54:01", "shopId": 2}""""
    producer.send(new ProducerRecord[String, String](topicName, "key", jsonString)).get()
    val service = new KafkaOffsetService(consumer, new OffsetRepository(null, null, null, null) {
      override def findOffsetInfo(topic: String): TopicOffsetInfo = null

      override def saveOffsetInfo(topicOffset: TopicOffsetInfo): Unit = {}
    })
    val (start, end) = service.getOffsets(topicName, 5000)
    val loader = KafkaLoader(spark, config, service, 5000, start, end)
    val df = loader.load(topicName)
    assert(df.count() > 0)
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

}
