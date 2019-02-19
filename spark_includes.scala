//Spark
import org.apache.spark.sql.{Row,SaveMode, SparkSession}
import org.apache.spark.sql.functions._ //{col, lit, to_timestamp, udf,concat,round}
import org.apache.spark.sql.expressions._ //Window
import org.apache.spark.sql.types.{StringType,DoubleType,DateType}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

//DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Duration,Period}

//Java
import java.util.regex.Pattern
import java.util.regex.Matcher

import java.util.Properties
  val connectionProperties = new Properties()
  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")

 df.write
      .format("org.elasticsearch.spark.sql")

val reader = spark.read
      .format("org.elasticsearch.spark.sql")

val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "words_new", "keyspace" -> "test_keyspace"))


