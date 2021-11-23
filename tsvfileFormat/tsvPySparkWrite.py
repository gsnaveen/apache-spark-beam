from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("createTsv").enableHiveSupport().getOrCreate()

output = './data/r2'

df = spark.createDataFrame([
    (1,"after the dog",'2021-11-12','2021-09-04 00:01:21')
,(2,"after the \n dogs 2 rows",'2021-11-12','2021-09-04 20:01:21')
,(3,'after the \n dogs 2 rows and quote " help ','2021-11-12','2021-09-04 20:01:21')
,(4,'after the \n dogs 2 rows and quote `@ help ','2021-11-12','2021-09-04 20:01:21')
,(5,'after the \n dogs 2 rows and \t tab special char ','2021-11-12','2021-09-04 22:01:21')
,(6,'after the \n dogs 2 rows and   tab ','2021-11-12','2021-09-04 23:01:21')
,(7,'after the \n dogs 2 rows and   未来有机会 ','2021-11-12','2021-09-04 23:01:21')
],['id','desc','dated','dtime'])

df_to_write = df
df_to_write = df_to_write.coalesce(1)
df_to_write.write.option("compression", "gzip").option("delimiter", "\t").csv(output)
