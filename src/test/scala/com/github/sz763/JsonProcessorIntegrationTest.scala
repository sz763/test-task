package com.github.sz763

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.DataFrame
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.MapHasAsJava

class JsonProcessorIntegrationTest extends AnyFunSuite with TestSparkSession {
  test("test processing json", Tag("integration")) {
    val path = Files.createTempDirectory("test-task").toFile.getAbsolutePath
    Files.createDirectory(Paths.get(path, "success"))
    val dictDf = dictionaryDf()
    val topicName = "integration_test_topic"
    val producer = new KafkaProducer[String, String](producerConfig().asJava)
    val config = consumerConfig()
    val consumer = new KafkaConsumer[String, String](config.asJava)
    sendMessage(topicName, producer, """{"id":1, "timestamp":"2022-01-01 20:54:01", "shopId": 2}"""")
    sendMessage(topicName, producer, """{"id":1, "timestamp":"2022-01-01 20:54:01", "shopId": 2}"""")
    sendMessage(topicName, producer, """{"id":2, "timestamp":"2022-01-01 20:54:01", "shopId": 10}"""")
    val service = new KafkaOffsetService(consumer, offsetRepository)
    val (start, end) = service.getOffsets(topicName, 5000)
    val loader = KafkaLoader(spark, config, service, 5000, start, end)
    val kafkaDf = loader.load(topicName)
    JsonProcessor(spark, path).process(dictDf, kafkaDf)
    val successDf = spark.read.parquet(path + "/success")
    successDf.count() should be(1)
    val failedDf = spark.read.parquet(path + "/failed")
    failedDf.count() should be(1)
    offsetRepository.saveOffsetInfo(end)
  }

  private def offsetRepository = {
    new OffsetRepository("jdbc:postgresql://localhost:5432/postgres", "sz", "1", "org.postgresql.Driver")
  }

  private def sendMessage(topicName: String, producer: KafkaProducer[String, String], message: String): Any = {
    producer.send(new ProducerRecord[String, String](topicName, "key", message)).get()
  }

  private def dictionaryDf(): DataFrame = {
    val dictionaryLoader = DictionaryLoader(spark,
      "jdbc:postgresql://localhost:5432/postgres", "sz", "1", "org.postgresql.Driver")
    dictionaryLoader.load("shops")
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
