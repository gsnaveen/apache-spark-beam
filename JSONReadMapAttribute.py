from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,MapType,StructField,StructType

spark = SparkSession.\
            builder.\
            appName("myJSONMapColumn").enableHiveSupport().getOrCreate()

data_schema = [StructField("RUNID",StringType(),True),StructField("mapAttribute", MapType(StringType(), StringType(), True),True)]
schemaStruct=StructType(fields=data_schema)

df = spark.read.json("./data/source/json/jsondatajson",schema=schemaStruct)
df.printSchema()

df.createOrReplaceTempView("dfSql")
dfsqlData = spark.sql("SELECT RUNID,Mapkey,MapValue FROM dfSql LATERAL VIEW explode(mapAttribute) cl AS Mapkey,Mapvalue")
dfsqlData.show()
