//Preparing the test data
var df = List(("2018-01-23","userid1",10),("2018-01-09","userid2",20),("2018-01-17","userid3",33)).toDF("viewdate","userid","visits")
//Computing Week of the Year
df = df.withColumn("weekofyear", weekofyear(col("viewdate")))
//Get minWeek
var minWeek = df.selectExpr("min(weekofyear)").collect()
df = df.withColumn("weekofyear", col("weekofyear") - minWeek(0)(0))
var StringAdd = "Week_"
df = df.withColumn("weekbucket", concat(lit(StringAdd),col("weekofyear").cast("string")))
//--Pivot to get weeks as columns
var dfPivot = df.groupBy("userid").pivot("weekbucket").sum("visits")
//DataFrame.na.fill()
dfPivot = dfPivot.na.fill(0)
