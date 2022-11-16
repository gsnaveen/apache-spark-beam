from pyspark.sql.functions import udf
from pyspark.sql.types import *

def getDomain(value):
	mydomain = value.split('/')
	if (len(mydomain) > 1):
		return  mydomain[2] 
	else: 
		return "NoDomain"

# sess =  dataframe with URL
  
udfgetDomain = udf(getDomain, StringType())
sess_with_domain = sess.withColumn("RequestDomain", udfgetDomain("url"))
sess_with_domain.show()
