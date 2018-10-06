import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat

/* Input Data is tab seperated file

custId	data_date
1	2018-08-01 11:00:00
2	2018-08-02 11:00:00
3	2018-08-03 11:00:00
4	2018-08-04 11:00:00

 */

object dtttest {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.appName("dateTest").master("local").getOrCreate(); //.enableHiveSupport()

    val innowDate: String = "2018-08-04"
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val gtDate = formatter.parseDateTime(innowDate).minusDays(2).toString("yyy-MM-dd")

    val df = spark.read.option("header","true").option("sep","\t").csv("inData.tsv")
    val df2 = df.withColumn("data_date_dtype", col("data_date").cast("date").cast("String"))
    val df3 = df2.filter(col("data_date_dtype").gt(gtDate))
//    val df3 = df2.filter(col("data_date_dtype") > gtDate)


    df3.show()
    println(df3.printSchema())
    spark.stop()
  }


}
