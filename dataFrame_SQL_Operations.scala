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
      , $"params"("linktext") as "action"
      )
      .withColumn("accpDecline", when($"action" === "accept", 1).otherwise( when($"action" === "decline", 0).otherwise(-1)))
      .filter($"accpDecline" === 0 || $"accpDecline" === 1)

import org.apache.spark.sql.expressions.Window
df1.withColumn("rank", rank().over(Window.partitionBy(col("viewdate")).orderBy(col("ip").asc))).show()
