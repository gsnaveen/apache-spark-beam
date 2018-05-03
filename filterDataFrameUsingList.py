from pyspark.sql.types import *
from pyspark.sql import functions as f

#-- Main DataFrame
df = spark.createDataFrame([("2018-01-23","userid1",10),("2018-01-09","userid2",20),("2018-01-17","userid3",33)], ['viewdate','userid','visits'])
#-- Filter Value DataFrame
dffilter = spark.createDataFrame([(1,"userid1"),(2,"userid2")],['id','userid'])
#-- Converting filter Values to a list
mylist = [row[0] for row in dffilter.select("userid").collect()]

#--Defining a function witrh filter list as default value
def filterUser(value,list1=mylist ):
		return True if value in list1 else False
#--Regestring UDF 	
udffilterUser = f.udf(filterUser, StringType
#--Applying the filter function
df = df.withColumn("filters", udffilterUser("userid"))

df.show()
