import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, Duration}

/*
custId	data_date	buy
1	2018-08-01 11:00:00	10
2	2018-08-02 11:00:00	15
3	2018-08-03 11:00:00	20
4	2018-08-04 11:00:00	30
*/

object datetest4 {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.appName("dateTest").master("local").getOrCreate(); //.enableHiveSupport()

    //Input date for computing the 8 weeks date range
    val innowDate: String = "2018-08-04"
    val lookBackWindow : Int = 2
    val lookBackWindowWeeks: Int = 8
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val getDateTime = formatter.parseDateTime(innowDate).minusDays(lookBackWindow).toString("yyyy-MM-dd")  //Computing based on the look back in term of days
    val getDatesforWeekStart = formatter.parseDateTime(innowDate).minusWeeks(lookBackWindowWeeks) //Computing based on the look back window in weeks

    def dateFilter(getDateTime: String)(tDate: String): Boolean = {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
      val formatterdt = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
      val myDuration: Duration = new Duration(formatter.parseDateTime(getDateTime), formatterdt.parseDateTime(tDate))

      myDuration.getStandardDays() match {
          // for filtering false gets filtered out
        case x if x > 0 => true
        case _ => false
      }
    }
    //Currying
    val dfDateFilter = dateFilter(getDateTime) _
    val dfDateFilterCall = udf[Boolean,String](dfDateFilter)


    //Reading the data from a text file
    val df = spark.read.option("header","true").option("sep","\t").csv("inData.tsv")
    val dfAns = df.filter(dfDateFilterCall(col("data_date"))).groupBy("custid").agg(sum(col("buy")).alias("total"),(sum(col("buy"))/8).alias("avg"))
    dfAns.show()

    //Printing Schema
    println(dfAns.printSchema())
    //Writing output as CSV
    dfAns.coalesce(1).write.mode("overwrite").option("header","true").csv("myoutput.csv")
    spark.stop()
  }

}
