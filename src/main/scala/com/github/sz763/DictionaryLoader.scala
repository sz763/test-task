package com.github.sz763

import org.apache.spark.sql.{DataFrame, SparkSession}

case class DictionaryLoader(spark: SparkSession, jdbcUrl: String, userName: String, password: String, dbDriver: String) {
  def load(tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password)
      .option("driver", dbDriver)
      .load()
  }
}
