import ReadingWeblogIntoDataFrameHelper._
import org.apache.spark.sql.SparkSession
/*
import scala.io._
import java.io._
import java.util.Date
import java.text.SimpleDateFormat
import java.net.URLDecoder
import java.util.UUID
 */

object webData {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.appName("WeblogReading").master("local").getOrCreate(); //.enableHiveSupport()
    spark.conf.set("spark.streaming.blockInterval", "20")
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    var myRDD = spark.read.text("file:///C:/tmp/da/mytestData.log").as[String] //textFile

    val df = myRDD.flatMap(weblogparser).as[webrow]
    df.show()
    spark.stop()
  }
}
