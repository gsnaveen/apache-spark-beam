import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
var df1 = spark.read.option("sep", "\t").option("header","true").csv("modthing.txt")
df1 = df1.withColumn("rowNum",row_number().over(Window.partitionBy(col("ip")).orderBy($"orders")) % 5)
var dfg = df1.groupBy(col("ip"),col("rowNum")).agg(sum(col("orders")).alias("sum1"))
dfg = dfg.groupBy(col("ip")).agg(sum(col("sum1")).alias("TotalSum"))
dfg.show()


// Redistributing data based on columns 
val repartDF = df1.repartition($"ip",$"rownum") 
repartDF.rdd.partitions.size
repartDF.write.csv("mypart1x")

df11.withColumn("id2",monotonicallyIncreasingId)
	  .withColumn("id2",row_number().over(Window.orderBy(lit(1))))
/*



+---+--------+
| ip|TotalSum|
+---+--------+
|  B|    55.0|
|  A|    55.0|
+---+--------+

+---+------+----+
| ip|rowNum|sum1|
+---+------+----+
|  B|     1| 6.0|
|  B|     2|16.0|
|  B|     3| 9.0|
|  B|     4|11.0|
|  B|     0|13.0|
|  A|     1| 6.0|
|  A|     2|16.0|
|  A|     3| 9.0|
|  A|     4|11.0|
|  A|     0|13.0|
+---+------+----+

ip	orders
A	1
A	2
A	3
A	4
A	5
A	6
A	7
A	8
A	9
A	10
B	1
B	2
B	3
B	4
B	5
B	6
B	7
B	8
B	9
B	10

*/
