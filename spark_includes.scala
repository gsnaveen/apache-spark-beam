import org.apache.spark.sql.{Row,SaveMode, SparkSession}
import org.apache.spark.sql.functions._ //{col, lit, to_timestamp, udf,concat,round}
import org.apache.spark.sql.expressions._ //Window
import org.apache.spark.sql.types.{StringType,DoubleType,DateType}

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Duration,Period}


import java.util.Properties
  val connectionProperties = new Properties()
  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")
