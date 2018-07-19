import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object session1 {
  case class sourceData(userid:String,t1:Double)
  case class SessionData(userid:String,t1:String,sessionid:String)

  def main(args: Array[String]) {
    var spark = SparkSession.builder.appName("Session1-Scala").master("local").getOrCreate(); //.enableHiveSupport()

    var records = Seq(sourceData("userid1",10),sourceData("userid1",25),sourceData("userid1",30),sourceData("userid1",65),sourceData("userid1",80),sourceData("userid1",88))
    var df = spark.createDataFrame(records)

    df = df.withColumn("nextt1", lead("t1",1,0).over(Window.partitionBy("userid").orderBy(col("t1").asc)))
    df = df.withColumn("beforet1", lag("t1",1,-99).over(Window.partitionBy("userid").orderBy(col("t1").asc)))

    df = df.withColumn("dwell",col("nextt1") - col("t1"))
    df = df.withColumn("diff",col("t1") - col("beforet1"))

    df = df.withColumn("SessStart" , when( col("beforet1") === -99 or col("diff") > 30, -99).when(col("nextt1") === 0,0)otherwise(col("dwell")))

    df = df.withColumn("ss",when( col("SessStart") === -99, concat(col("userid"),col("t1").cast("string"))))

    var dfwork = df.select(col("userid"),col("t1"),col("ss"))

    var dfworkArray = dfwork.collect()

    var SessionID = dfworkArray(0)(2).toString
    var mydataall = spark.createDataFrame(Seq(SessionData(dfworkArray(0)(0).toString,dfworkArray(0)(1).toString,SessionID)))

    for (i <- 1 to dfworkArray.size -1 ) {

      if (dfworkArray(i)(2) != null)
      {
        SessionID = dfworkArray(i)(2).toString
      }

      mydataall = mydataall.union(spark.createDataFrame(Seq(SessionData(dfworkArray(i)(0).toString,dfworkArray(i)(1).toString,SessionID))))
    }

    mydataall.show()

    spark.stop()
  }
}
