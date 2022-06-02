package com.github.sz763

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

case class KafkaLoader(spark: SparkSession,
                       config: Map[String, Object],
                       kafkaOffsetService: KafkaOffsetService,
                       batchSize: Int,
                       startOffset: TopicOffsetInfo,
                       endOffset: TopicOffsetInfo
                      ) extends LazyLogging {
  def load(topicName: String): DataFrame = {
    logger.info("Loading the data from offset {} to {}", startOffset, endOffset)
    spark.read
      .format("kafka")
      .option("subscribe", topicName)
      .option("kafka.bootstrap.servers", configValue(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
      .option("kafka.group.id", configValue(ConsumerConfig.GROUP_ID_CONFIG))
      .option("startingOffsets", startOffset.toString)
      .option("endingOffsets", endOffset.toString)
      .load()
  }

  private def configValue(key: String): String = config(key).toString
}
