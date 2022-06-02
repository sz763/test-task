package com.github.sz763

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, DriverManager}
import scala.util.{Failure, Success, Using}

class OffsetRepository(jdbcUrl: String, user: String, password: String, driverClass: String) extends LazyLogging {
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  if (driverClass != null) {
    Class.forName(driverClass)
  } else {
    logger.warn("Driver class name is null")
  }


  def findOffsetInfo(topic: String): TopicOffsetInfo = {
    logger.info("Fetch offsets from database for topic: '{}'", topic)
    val offset = Using(jdbcConnection()) { connection =>
      val statement = connection.prepareStatement("select offsets from topic_offsets where topic_name = ?")
      try {
        statement.setString(1, topic)
        val resultSet = statement.executeQuery()
        if (!resultSet.next()) {
          return null
        }
        objectMapper.readValue(resultSet.getString("offsets"), classOf[TopicOffsetInfo])
      } finally statement.close()
    }
    val result = offset match {
      case Success(value) => value
      case Failure(exception) =>
        throw new IllegalStateException(s"Not able to load offset from db for topic $topic", exception)
    }
    logger.info("Fetched offset for topic '{}' value: {}", offset.toString)
    result
  }

  def saveOffsetInfo(topicOffset: TopicOffsetInfo): Unit = {
    val offsetString = objectMapper.writeValueAsString(topicOffset)
    val query = "insert into topic_offsets(topic_name, offsets) values(?, ?) on conflict(topic_name) do update set offsets = ?"
    Using(jdbcConnection()) { connection =>
      val statement = connection.prepareStatement(query)
      try {
        statement.setString(1, topicOffset.topic)
        statement.setString(2, offsetString)
        statement.setString(3, offsetString)
        statement.executeUpdate()
      } finally statement.close()
    }
  }

  def jdbcConnection(): Connection = {
    DriverManager.getConnection(jdbcUrl, user, password)
  }

}