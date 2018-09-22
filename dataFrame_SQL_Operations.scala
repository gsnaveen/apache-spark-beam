//Group by Multiple columns, Alias and creating multiple metrics
df.groupBy(col("viewdate").alias("dim1"),col("country_code")).agg(min("viewdate").alias("Min"),max("viewdate").alias("Max")).show()

//Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left'
//, 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'leftanti', 'left_anti', 'cross'
val df3 = df1.join(df2, df1.col("viewdate") === df2.col("viewdate") && df1.col("ip") ===df2.col("ip"),"inner")
                  .select(df1.col("ip").alias("df1IP"),df2.col("ip").alias("df2IP"),df1.col("viewdate"),df2.col("viewdate"))
val df3 = df1.join(df2, df1.col("viewdate") === df2.col("viewdate") && df1.col("ip") ===df2.col("ip"),"left_outer")
                  .select(df1.col("ip").alias("df1IP"),df2.col("ip").alias("df2IP"),df1.col("viewdate"),df2.col("viewdate"))

.filter(df1.col("viewdate") === "2017-08-17" || df1.col("viewdate") === "2017-08-18" )
.filter(df1.col("viewdate") === "2017-08-17" && df1.col("viewdate") === "2017-08-18" )

val acceptDecline = spark.table("MyTable")
      .select(
        $"date" cast "date" as "viewdate"
      , $"user" as "userid"
      , $"params"("event") as "action"
      )
      .withColumn("accpDecline", when($"action" === "accept", 1).otherwise( when($"action" === "decline", 0).otherwise(-1)))
      .filter($"accpDecline" === 0 || $"accpDecline" === 1)

// Joining using a broadcast variable
import org.apache.spark.sql.functions.broadcast
val df1 = spark.read.option("sep", "\t").option("header","true").csv("inData.txt")
val df22 = spark.read.option("sep", "\t").option("header","true").csv("inData2.txt")
val df2 = broadcast(df22)

//Windowing functions
import org.apache.spark.sql.expressions.Window
df1.withColumn("rank", rank().over(Window.partitionBy(col("viewdate")).orderBy(col("ip").asc))).show()

// SQL Aggregate functions
select RowKey
,session_id
,datetime
,url
,FIRST_VALUE(url) over( partition by session_id order by `timestamp`)  firstURL
,LAST_VALUE(url) over( partition by session_id order by `timestamp`)  LastURL
,rank() over( partition by session_id order by `timestamp`)  rank1
,dense_rank() over( partition by session_id order by `timestamp`)  denserank
,ROW_NUMBER() over( partition by session_id order by `timestamp`) rownumber
,CUME_DIST() over( partition by session_id order by `timestamp`) cumedist
from Webdata
