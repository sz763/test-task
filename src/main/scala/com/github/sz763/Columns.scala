package com.github.sz763

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object Columns {
  object Shop {
    val ID = "id"
    val NAME = "name"
    val $ID: Column = col("id")
    val $NAME: Column = col("name")
  }

  object Order {
    val ID = "id"
    val ORDER_TIMESTAMP = "order_timestamp"
    val RECEIVE_TIME = "receive_time"
    val SHOP_ID = "shop_id"
    val SHOP_NAME = "shop_name"
    val WEEK_NUMBER = "week_number"
    val YEAR = "year"
    val $ID: Column = col(ID)
    val $SHOP_NAME: Column = col(SHOP_NAME)
    val $WEEK_NUMBER: Column = col(WEEK_NUMBER)
    val $RECEIVE_TIME: Column = col(RECEIVE_TIME)
    val $YEAR: Column = col("year")
  }

}
