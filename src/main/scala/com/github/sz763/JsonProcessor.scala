package com.github.sz763

import com.github.sz763.Columns.Order.RECEIVE_TIME
import com.github.sz763.Columns.{Order, Shop}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{from_json, weekofyear, year}
import org.apache.spark.sql.types.{LongType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class JsonProcessor(sparkSession: SparkSession, writeTo: String, batchSize: Int = 5000) extends LazyLogging {
  private val orderStruct = new StructType()
    .add("id", LongType)
    .add("timestamp", TimestampType)
    .add("shopId", LongType)
  private val spark = sparkSession

  import spark.implicits._


  def process(dictionary: DataFrame, ordersKafkaDf: DataFrame): Unit = {
    val ordersDf = jsonToFlatTable(ordersKafkaDf)
    val deduplicated = deduplicate(ordersDf)
    val resultDf = enrichWithDictionary(deduplicated, dictionary)
    storeResultDf(resultDf.cache())
  }

  def deduplicate(resultDf: DataFrame, storedRecords: DataFrame): DataFrame = {
    val existingDf = storedRecords.select(Order.ID, Order.YEAR, Order.WEEK_NUMBER)
    resultDf.join(existingDf, Seq(Order.ID, Order.YEAR, Order.WEEK_NUMBER), "left_anti")
    //TODO we can add store duplicates to extra storage
  }


  def deduplicate(resultDf: DataFrame): DataFrame = {
    val sourceDf = resultDf.cache()
    val sourceCount = sourceDf.count()
    val df = sourceDf.dropDuplicates(Order.ID)
    try {
      val storedRecords = spark.read.parquet(resolvePath("success"))
      val dataFrame = deduplicate(df, storedRecords).cache()
      logger.info("{} duplicates found", sourceCount - dataFrame.count())
      dataFrame
    } catch {
      case _: Exception => df //TODO log an error
    }
  }

  def storeResultDf(resultDf: DataFrame): Unit = {
    val success = resultDf.filter(Order.$SHOP_NAME.isNotNull).cache()
    storeResults(success, "success")
    logger.info("Success count: {}", success.count())
    val failed = resultDf.filter(Order.$SHOP_NAME.isNull).cache()
    storeResults(failed, "failed")
    logger.info("Failed count: {}", failed.count())
  }

  def storeResults(resultDf: DataFrame, path: String): Unit = {
    resultDf
      .repartition(Order.$YEAR, Order.$WEEK_NUMBER)
      .write
      .mode(SaveMode.Append)
      .partitionBy(Order.YEAR, Order.WEEK_NUMBER)
      .parquet(resolvePath(path))
  }

  def resolvePath(path: String): String = {
    if (writeTo.endsWith("/")) {
      writeTo + path
    } else {
      writeTo + "/" + path
    }
  }

  def jsonToFlatTable(kafkaDf: DataFrame): DataFrame = {
    kafkaDf.withColumn("json", from_json($"value".cast("string"), orderStruct))
      .select(
        $"json.id".as(Order.ID),
        $"json.shopId".as(Order.SHOP_ID),
        $"timestamp".cast(TimestampType).alias(Order.RECEIVE_TIME),
        $"json.timestamp".as(Order.ORDER_TIMESTAMP),
      )
      .withColumn(Order.YEAR, year(Order.$RECEIVE_TIME))
      .withColumn(Order.WEEK_NUMBER, weekofyear(Order.$RECEIVE_TIME))
  }

  def enrichWithDictionary(ordersDf: DataFrame, shopDf: DataFrame): DataFrame = {
    ordersDf.as("o")
      .join(shopDf.as("s"), ordersDf.col(Order.SHOP_ID) === shopDf.col(Shop.ID), "left")
      .select(
        ordersDf.col(RECEIVE_TIME),
        ordersDf.col(Order.ID),
        ordersDf.col(Order.ORDER_TIMESTAMP),
        shopDf.col(Shop.NAME).as(Order.SHOP_NAME),
        ordersDf.col(Order.YEAR),
        ordersDf.col(Order.WEEK_NUMBER)
      ).orderBy(Order.YEAR, Order.WEEK_NUMBER)
  }

}
