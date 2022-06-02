package com.github.sz763

import com.github.sz763.Columns.{Order, Shop}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, convertToAnyMustWrapper, equal}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Files

class JsonProcessorTest extends AnyFunSuite with TestSparkSession {

  import spark.implicits._

  test("test convert json to flat table") {
    val processor = JsonProcessor(spark, null)
    val kafkaDf = Seq(("key", """{"id":1, "timestamp":"1970-01-01 20:54:01 ", "shopId": 2}""", "1970-01-01 00:00:00"))
      .toDF("key", "value", "timestamp")
    val row = processor.jsonToFlatTable(kafkaDf).collect()(0)
    val structType = new StructType()
      .add(StructField(Order.ID, LongType))
      .add(StructField(Order.SHOP_ID, LongType))
      .add(StructField(Order.RECEIVE_TIME, TimestampType))
      .add(StructField(Order.ORDER_TIMESTAMP, TimestampType))
      .add(StructField(Order.YEAR, IntegerType))
      .add(StructField(Order.WEEK_NUMBER, IntegerType))
    row.schema must equal(structType)
  }

  test("test store success partitioned record") {
    import spark.implicits._
    val path = Files.createTempDirectory("test-task").toFile.getAbsolutePath
    val processor = JsonProcessor(spark, path)
    val dataFrame = Seq(("1970-01-01 20:54:01", "shop#1", 1970, 1))
      .toDF(Order.RECEIVE_TIME, Order.SHOP_NAME, Order.YEAR, Order.WEEK_NUMBER)
    processor.storeResultDf(dataFrame)
    val ar = spark.read.parquet(path + "/success")
    val row = ar.collect()(0)
    row.getAs(Order.SHOP_NAME).asInstanceOf[String] must equal("shop#1")
  }

  test("test store failed partitioned record") {
    val path = Files.createTempDirectory("test-task").toFile.getAbsolutePath
    val processor = JsonProcessor(spark, path)
    val dataFrame = Seq(("1970-01-01 20:54:01", 1970, 1))
      .toDF(Order.RECEIVE_TIME, Order.YEAR, Order.WEEK_NUMBER)
      .withColumn(Order.SHOP_NAME, lit(null).cast("string"))
    processor.storeResultDf(dataFrame)
    val ar = spark.read.parquet(path + "/failed")
    val row = ar.collect()(0)
    row.getAs(Order.SHOP_NAME).asInstanceOf[String] should be(null)
  }

  test("test resolvePath adds '/'") {
    val processor = JsonProcessor(spark, "hdfs://localhost:9000/test/path")
    val str = processor.resolvePath("success")
    str must equal("hdfs://localhost:9000/test/path/success")
  }

  test("test enrich data with dictionary") {
    val path = Files.createTempDirectory("test-task").toFile.getAbsolutePath
    val processor = JsonProcessor(spark, path)
    val kafkaDf = Seq(
      ("key", """{"id":1, "timestamp":"2022-01-01 20:54:01", "shopId": 2}""", "1970-01-01 00:00:00"),
      ("key", """{"id":2, "timestamp":"2022-01-03 20:54:01", "shopId": 10}""", "1970-01-07 00:00:00"),
      ("key", """{"id":3, "timestamp":"2022-01-02 20:54:01", "shopId": 2}""", "1970-01-14 00:00:00"),
    ).toDF("key", "value", "timestamp")
    val ordersDf = processor.jsonToFlatTable(kafkaDf)
    val shopDf = Seq((2, "shop#2")).toDF(Shop.ID, Shop.NAME)
    val dataFrame = processor.enrichWithDictionary(ordersDf, shopDf)
    val rows = dataFrame.collect()
    rows(0).getAs(Order.SHOP_NAME).asInstanceOf[String] must equal("shop#2")
    rows(1).getAs(Order.SHOP_NAME).asInstanceOf[String] must equal(null)
    rows(2).getAs(Order.SHOP_NAME).asInstanceOf[String] must equal("shop#2")
  }

  test("deduplicate records") {
    val processor = JsonProcessor(spark, "")
    val events = Seq(
      (1L, 2L, "2022-01-01 20:54:01", 2022, 1),
      (2L, 2L, "2022-01-01 20:54:01", 2022, 1),
      (3L, 2L, "2022-01-01 20:54:01", 2022, 1)
    ).toDF(Order.ID, Order.SHOP_ID, Order.RECEIVE_TIME, Order.YEAR, Order.WEEK_NUMBER)
    val existing = Seq((2L, 2L, "2022-01-01 20:54:01", 2022, 1))
      .toDF(Order.ID, Order.SHOP_ID, Order.RECEIVE_TIME, Order.YEAR, Order.WEEK_NUMBER)
    val result = processor.deduplicate(events, existing)
    val rows = result.orderBy(Order.ID).collect()
    result.count() must be(2)
    rows(0).getAs(Order.ID).asInstanceOf[Long] must equal(1L)
    rows(1).getAs(Order.ID).asInstanceOf[Long] must equal(3L)
  }

}
