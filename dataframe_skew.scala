import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
var df1 = spark.read.option("sep", "\t").option("header","true").csv("modthing.txt")
df1 = df1.withColumn("rowNum",row_number().over(Window.partitionBy(col("ip")).orderBy($"orders")) % 5)
var dfg = df1.groupBy(col("ip"),col("rowNum")).agg(sum(col("orders")).alias("sum1"))
dfg = dfg.groupBy(col("ip")).agg(sum(col("sum1")).alias("TotalSum"))
dfg.show()
