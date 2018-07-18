import org.apache.spark.sql.expressions.Window

var df = List(("userid1",10),("userid1",25),("userid1",30),("userid2",10),("userid2",25),("userid2",30)).toDF("userid","t1")

df = df.withColumn("nextt1", lead("t1",1,0).over(Window.partitionBy("userid").orderBy($"t1".asc)))
df = df.withColumn("beforet1", lag("t1",1,-99).over(Window.partitionBy("userid").orderBy($"t1".asc)))
df = df.withColumn("diff",col("nextt1") - col("t1"))

df = df.withColumn("ss",when( col("beforet1") === -99, concat(col("userid"),col("t1").cast("string"))))

df = df.withColumn("sessionid",when( col("beforet1") < 0, col("ss")).when( col("beforet1") > -1, lag("ss",1).over(Window.partitionBy("userid").orderBy($"t1".asc))))
