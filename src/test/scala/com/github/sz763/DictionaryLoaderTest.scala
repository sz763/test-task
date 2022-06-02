package com.github.sz763

import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{convertToAnyMustWrapper, equal}

class DictionaryLoaderTest extends AnyFunSuite with TestSparkSession {

  import spark.implicits._

  test("test load data from dictionary", Tag("Integration")) {
    val dictionaryLoader = DictionaryLoader(spark,
      "jdbc:postgresql://localhost:5432/postgres", "sz", "1", "org.postgresql.Driver")
    val dataFrame = dictionaryLoader.load("shops")
    val name = dataFrame.filter($"id" === 3).select("name")
      .collect()(0).getAs("name").asInstanceOf[String]
    name must equal("shop#3")
  }

}
