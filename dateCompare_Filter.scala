import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat

/*
custId	data_date	buy
1	2018-08-01 11:00:00	10
2	2018-08-02 11:00:00	15
3	2018-08-03 11:00:00	20
4	2018-08-04 11:00:00	30
*/

object datetest2 {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.appName("dateTest").master("local").getOrCreate(); //.enableHiveSupport()

    val innowDate: String = "2018-08-04"
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val gtDate = formatter.parseDateTime(innowDate).minusDays(2).toString("yyy-MM-dd") //56 days for the customer scenario

    val df = spark.read.option("header","true").option("sep","\t").csv("inData.tsv")
    val dfAns = df.filter(col("data_date").gt(gtDate)).groupBy("custid").agg(sum(col("buy")).alias("total"),(sum(col("buy"))/8).alias("avg")) //takes into consideration the time component of the date as well
    val dfAns2 = df.filter(col("data_date").cast("date").cast("String").gt(gtDate)).groupBy("custid").agg(sum(col("buy")).alias("total"),(sum(col("buy"))/8).alias("avg")) // this takes only the date into consideration

    println("Date only grater than")
    dfAns.show()

    println("Date time grater than")
    dfAns2.show()

    println(dfAns.printSchema())
    dfAns.coalesce(1).write.mode("overwrite").option("header","true").csv("myoutput.csv")
    spark.stop()
  }

}
