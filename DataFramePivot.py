from pyspark.sql import functions as f
df = spark.createDataFrame([("2018-01-23","userid1",10),("2018-01-09","userid2",20),("2018-01-17","userid3",33)], ['viewdate','userid','visits'])
df = df.withColumn('weekofyear', f.weekofyear(df.viewdate))
minWeek = df.selectExpr("min(weekofyear)").collect()

df = df.withColumn('weekofyear', df.weekofyear - minWeek[0][0])
StringAdd = 'Week_'
df = df.withColumn("weekbucket", f.concat(f.lit(StringAdd),df.weekofyear.cast("string")))

#--Pivot to get weeks as columns
dfPivot = df.groupBy("userid").pivot("weekbucket").sum("visits")

#--Filling NA with 0
dfPivot = dfPivot.fillna(0)
