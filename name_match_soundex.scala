// Creating base data frame
var dfx = List("JPMC","JP Morgan Chase","JP Chase","JP Morgan").toDF("names")
dfx = dfx.withColumn("sounded",soundex(col("names"))) // Appying soundex function to the names attribute
dfx = dfx.withColumn("letpart",substring(col("sounded"),1,1)) //Getting letter part of the sounded attribute
dfx = dfx.withColumn("numpart",expr("substring(sounded, 2, length(sounded)-1)")) //Getting number part of the sounded attribute

// Creating new data/to be mapped dataframe
var dfy = List("JP Morgan Chase","JPMC","JPMD","XJPMD").toDF("names")
dfy = dfy.withColumn("sounded",soundex(col("names")))
dfy = dfy.withColumn("letpart",substring(col("sounded"),1,1))
dfy = dfy.withColumn("numpart",expr("substring(sounded, 2, length(sounded)-1)"))

//Joining base and new data dataframe update the number in geq & leq as per your tolerance  
var dataset = dfy.join(dfx,dfy("letpart") === dfx("letpart") && dfx("numpart").geq(dfy("numpart") - 10) && dfx("numpart").leq(dfy("numpart") + 10) ).select(dfy.col("names").alias("namesy"),dfx.col("names").alias("namesx"),dfy.col("sounded"),dfx.col("sounded"),dfy.col("numpart").alias("numparty"),dfx.col("numpart").alias("numpartx"))

//Drop duplicates as there could be many duplicate records
dataset = dataset.dropDuplicates()
dataset = dataset.withColumn("dist",abs(col("numpartx") - col("numparty"))) // Compute the absolute difference between the number part from source and new dataset

//Import Scala libs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

//Get the least absolute difference values 
var datasetleast = dataset.groupBy(dataset.col("namesy").alias("minnamesy")).agg(min("dist").alias("minval"))

//join the datasetleast with main dataset
var datasetjoined = datasetleast.join(dataset,datasetleast("minnamesy") === dataset("namesy") && datasetleast("minval") === dataset("dist"), "inner").select(datasetleast.col("minnamesy"),dataset.col("namesx"),dataset.col("dist"),datasetleast.col("minval"),dataset.col("numpartx"),dataset.col("numparty"))


datasetjoined = datasetjoined.withColumn("rank", rank().over(Window.partitionBy("minnamesy").orderBy($"minval".asc))) //rank the rows
datasetjoined = datasetjoined.withColumn("rankRow", row_number().over(Window.partitionBy("minnamesy").orderBy($"minval".asc))) //get row numbers

//Pick the first row for each joined row
datasetjoined = datasetjoined.filter(datasetjoined("rankRow") === 1)

//levenshtein distance, for to know how different are the match
datasetjoined = datasetjoined.withColumn("levenshteinDist",levenshtein(col("minnamesy"), col("namesx")))
