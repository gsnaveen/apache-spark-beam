from pyspark.sql.functions import udf
from pyspark.sql.types import *

def getDomain(value):
	mydomain = value.split('/')
	if (len(mydomain) > 1):
		return  mydomain[2] 
	else: 
		return "NoDomain"

# sess =  dataframe with URL

sess = spark.createDataFrame([
     ('//www.mycomp.om/x/a/'),
     ('//www.mycomp.om/x/b/'),
     ('//www.mycomp.om/x/c/')
 ], ["url"])
  
udfgetDomain = udf(getDomain, StringType())
sess_with_domain = sess.withColumn("RequestDomain", udfgetDomain("url"))
sess_with_domain.show()

# Another example with complex data type
def mystructcall (val1,val2):
    mystruct = {"refreshopp":None,"servicerenewal":None}
    mystruct["refreshopp"] = val1
    mystruct["servicerenewal"] = val2
    return mystruct
	
mystructcall = udf(mystructcall, StructType().add("refreshopp", IntegerType(), True).add("servicerenewal", IntegerType(), True, None))
df_with_struct = df.withColumn("mystruct1", mystructcall("refreshopp","servicerenewal"))
