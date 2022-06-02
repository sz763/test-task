package com.github.sz763

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Using


object SparkApplication extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("Start processing orders")
    val config: Map[String, Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> System.getenv().get("BOOTSTRAP_SERVERS"),
      ConsumerConfig.CLIENT_ID_CONFIG -> System.getenv().get("CLIENT_ID"),
      ConsumerConfig.GROUP_ID_CONFIG -> System.getenv().get("GROUP_ID"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
    )
    val topicName = System.getenv("TOPIC_NAME")
    val batchSize = System.getenv("BATCH_SIZE").toInt
    val parquetBatchSize = System.getenv("PARQUET_BATCH_SIZE").toInt
    val writeTo = System.getenv("WRITE_TO")
    val jdbcUrl = System.getenv().get("JDBC_URL")
    val dbUser = System.getenv().get("DB_USER")
    val dbPassword = System.getenv().get("DB_PASSWORD")
    val dbDriver = System.getenv().get("DB_DRIVER")

    Using(SparkSession.builder().appName("test-task").getOrCreate()) { spark =>
      val processor = JsonProcessor(spark, writeTo, parquetBatchSize)
      val dictionaryLoader = DictionaryLoader(spark, jdbcUrl, dbUser, dbPassword, dbDriver)
      val offsetRepository = new OffsetRepository(jdbcUrl, dbUser, dbPassword, dbDriver)
      val kafkaOffsetService = new KafkaOffsetService(new KafkaConsumer[String, String](config.asJava), offsetRepository)
      val (startOffset, endOffset) = kafkaOffsetService.getOffsets(topicName, batchSize)
      val kafkaLoader = KafkaLoader(spark, config, kafkaOffsetService, batchSize, startOffset, endOffset)
      processor.process(dictionaryLoader.load("shops"), kafkaLoader.load(topicName))
      offsetRepository.saveOffsetInfo(endOffset)
      logger.info("Orders processing has been finished")
    }

  }
}
