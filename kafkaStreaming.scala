import ReadingWeblogIntoDataFrameHelper._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/*
scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
*/

object SparkKafka {


  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.appName("StructuredStreaming").master("local").getOrCreate(); //.enableHiveSupport()
    import sparkSession.implicits._

    val kafkaDStream = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testLogs1")
      .load()
      .selectExpr("CAST(value AS STRING)")


    //val kafkaDStream1 = kafkaDStream.toDF("value").as[String].flatMap(weblogparser).as[webrow]
    //val kafkaDStream1 = kafkaDStream.as[String].flatMap(weblogparser).as[webrow] //for writing to HDFS or Streaming processes
    val kafkaDStream1 = kafkaDStream.toDF("value").as[String] //for Writing to the Kafka Queue it needs a value column
    
// For writing to the console    
//        val query = kafkaDStream1.writeStream
//          .trigger(Trigger.Continuous(3000))
//          .format("console")
//          .outputMode(OutputMode.Append())
//          .start()

    //For writing to the KAFKA Queue. The dataFrame should have column name value.
    val query = kafkaDStream1.writeStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("topic", "update")
                  .option("checkpointLocation","C:\\tmp")
                  .start()

    query.awaitTermination()

  }
}
