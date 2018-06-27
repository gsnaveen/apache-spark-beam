case class dfSchema(cookie:String,url:String,campaign:String)

//Reading as RDD and converting it to a dataframe
val myRDD = spark.sparkContext.textFile("/nav_sp1/*")
val counts = myRDD.map(line => (line.split("\t")(0),1)).reduceByKey(_ + _)
def myparser(x:String) : Option[dfSchema] = {
    val mypattern = x.split('\t');
    return Some(dfSchema(mypattern(0),mypattern(1),mypattern(2)))
  }

val df = myRDD.flatMap(myparser).toDF()

//Reading as DataSet and converting it to a dataframe

// as[String is critical to have this as a data set]  
var myDS = spark.read.text("/nav_sp1/*").as[String]
//myDS: org.apache.spark.sql.Dataset[String] = [value: string]
val df = myDS.flatMap(myparser)

//import as DataFrame myparser(x:Row)  x has to be rowType
var myDF = spark.read.text("/nav_sp1/*")
//myDF: org.apache.spark.sql.DataFrame = [value: string]

def myparser(x:Row) : Option[dfSchema] = {
    val mypattern = x.toString().split('\t');
    return Some(dfSchema(mypattern(0),mypattern(1),mypattern(2)))
 }
 
val df = myDF.flatMap(myparser)
