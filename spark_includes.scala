//Spark
import org.apache.spark.sql.{Row,SaveMode, SparkSession}
import org.apache.spark.sql.functions._ //{col, lit, to_timestamp, udf,concat,round}
import org.apache.spark.sql.expressions._ //Window
import org.apache.spark.sql.types.{StringType,DoubleType,DateType,StructType}
import org.apache.spark.util._ //SizeEstimator.estimate(df3)  println(df4.rdd.partitions.size) println(df4.rdd.getNumPartitions)

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

//DateTime
import org.joda.time.format.DateTimeFormat //forPattern("yyyy-MM-dd") parseDateTime(vriable)
import org.joda.time.{DateTime, Duration,Period,Interval,Days,Months, Years}

//Java
import java.util.regex.Pattern
import java.util.regex.Matcher

//Postgres
val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

// Create a Properties() object to hold the parameters.
val connectionProperties = new Properties()
connectionProperties.put("user", s"${jdbcUsername}")
connectionProperties.put("password", s"${jdbcPassword}")
val schema1table = spark.read.jdbc(jdbcUrl, "schema1.testdata", connectionProperties)
schema1table.repartition(1).write.mode(SaveMode.Append).jdbc(jdbcUrl, "schema1.diamonds", connectionProperties)

//Elastic
 df.write
      .format("org.elasticsearch.spark.sql")

val reader = spark.read
      .format("org.elasticsearch.spark.sql")

//Cassandra
val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "words_new", "keyspace" -> "test_keyspace"))



spark.conf.set("spark.sql.shuffle.partitions", 1000)
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
// spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",20971520)


libraryDependencies += "junit" % "junit" % "4.12" % Test
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"  % "test"
libraryDependencies += "joda-time" % "joda-time" % "2.10"

libraryDependencies ++= {
val sparkVer = "2.3.0"
  Seq(
        "org.apache.spark" %% "spark-core" % sparkVer ,
        "org.apache.spark" %% "spark-sql" % sparkVer ,
    "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
    "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
  )
}

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.1.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.10"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.0"
