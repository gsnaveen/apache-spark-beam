
val counter = sc.longAccumulator("counter")
counter.add(10)

val df22 = spark.read.option("sep", "\t").option("header","true").csv("inData2.txt")
val df2 = broadcast(df22)

//Writing to given number of files

df.coalesce(5).write.csv("/app/db/folder")		# will create only 5 files which may not be equal in size.
df1 = df1.coalesce(7)
df1.rdd.partitions.size 

val df1 = df1.repartition(5)
locationDf = peopleDf.repartition($"location") //Creates 200 Partiontions some will be empty

val df1Sample = df1.sample(true, 0.10).coalesce(1)

val columns = Seq("level1","level2")
df1.write.partitionBy(columns:_*).mode("overwrite").save(s"$hive_warehouse/$dbname.db/$temp_table/")
