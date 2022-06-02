package com.github.sz763

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestSparkSession extends BeforeAndAfterAll {
  this: Suite =>

  lazy val spark: SparkSession = _ss
  @transient private var _ss: SparkSession = _

  protected val TestName: String = getClass.getCanonicalName

  override def beforeAll(): Unit = {
    super.beforeAll()
    _ss = SparkSession.builder
      .appName(TestName)
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}