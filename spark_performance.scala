import org.apache.spark.util._

var spark = SparkSession.builder.appName("blukai").master("local").getOrCreate();
val sqlContext = spark.sqlContext
import sqlContext.implicits._
val myData  = spark.sparkContext.textFile("mydata.txt") //.as[String]

//Displays the size of the object in bytes
println(SizeEstimator.estimate(df3))

spark-submit 
"""
--master yarn 
--deploy-mode cluster 
--driver-memory 9g 
--num-executors 50 // 50 * 12 = 600GB   OVERHEAD = max(SPECIFIED_MEMORY * 0.07, 384M) = 42 GB is overhead
--executor-memory 12g 
--conf spark.sql.shuffle.partitions=500 
--executor-cores 5 // 5 threads per executor which is good for HDFS
--class "com.Main"  
--files ./hive-site.xml ./myfile.jar '2017-06-26' '2017-08-20'
"""


println(df3.explain(true)) // Will explain the spark process
"""
// == Optimized Logical Plan ==
// Project [custId#10, data_date#11, buy#12, cast(data_date#11 as date) AS data_date_dtype#16]
// +- Filter (isnotnull(data_date#11) && (cast(cast(data_date#11 as date) as string) > 2018-08-02))
//    +- Relation[custId#10,data_date#11,buy#12] csv

// == Physical Plan ==
// *(1) Project [custId#10, data_date#11, buy#12, cast(data_date#11 as date) AS data_date_dtype#16]
// +- *(1) Filter (isnotnull(data_date#11) && (cast(cast(data_date#11 as date) as string) > 2018-08-02))
//    +- *(1) FileScan csv [custId#10,data_date#11,buy#12] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/myScalaA/bk1/inData.tsv], PartitionFilters: [], PushedFilters: [IsNotNull(data_date)], ReadSchema: struct<custId:string,data_date:string,buy:string>
"""
df.coalesce(5).write.csv("/app/db/folder")		// will create only 5 files which may not be equal in size.
df1 = df1.coalesce(7)
df1.rdd.partitions.size 
df1.rdd.getNumPartitions

val df1 = df1.repartition(5)
val locationDf = peopleDf.repartition($"location") //Creates 200 Partiontions some will be empty
val dfpart df.repartition($"key", 2).sortWithinPartitions() // repartition based on the key

number_of_partitions = number_of_cpus * (dataSplits) //

val df1Sample = df1.sample(true, 0.10).coalesce(1)

val counter = sc.longAccumulator("counter")
counter.add(10)


