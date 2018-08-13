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
    val kafkaDStream1 = kafkaDStream.as[String].flatMap(weblogparser).as[webrow]

        val query = kafkaDStream1.writeStream
          .trigger(Trigger.Continuous(3000))
          .format("console")
          .outputMode(OutputMode.Append())
          .start()


    query.awaitTermination()

  }
}
