//Writing to given number of files

df.coalesce(5).write.csv("/app/db/folder")		# will create only 5 files which may not be equal in size.
df1 = df1.coalesce(7)
df1.rdd.partitions.size 

val df1 = df1.repartition(5)
locationDf = peopleDf.repartition($"location") //Creates 200 Partiontions some will be empty

val df1Sample = df1.sample(true, 0.10).coalesce(1)
