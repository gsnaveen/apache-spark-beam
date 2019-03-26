from pyspark.sql import SparkSession
from datetime import datetime, timedelta

start_date = '2018-01-01'
end_date = '2019-03-31'

cdt = datetime.strptime(start_date, '%Y-%m-%d').date()
edt = datetime.strptime(end_date, '%Y-%m-%d').date()
spark = SparkSession.builder.appName("incremental").enableHiveSupport().getOrCreate()

mappedData = spark.sql("""Select uc.userid,uc.cookie from mydb1.cookiemap uc inner join mydb1.userlist usr
                            on lower(usr.userid) = lower(uc.userid)""")

mappedData.createOrReplaceTempView("mappedData")

database = spark.sql("""Select web.* , uc.userid as cco_id from mydb1.web_data web inner join mappedData uc 
                            on uc.cookie = web.unique_visitor
                            where web.viewdate ='""" + str(cdt) + """' limit 100""")

database.write.saveAsTable("mydb1.incrementalData", mode='overwrite', format='orc', compression='snappy')

cdt = cdt + timedelta(days=1)
while cdt <= edt:
    data1 = spark.sql("""Select web.* , uc.userid as cco_id from mydb1.web_data web inner join mappedData uc 
                                on uc.cookie = web.unique_visitor
                                where web.viewdate ='""" + str(cdt) + """' limit 100""")

    data1.write.saveAsTable("mydb1.incrementalData", mode='append', format='orc', compression='snappy')

    cdt = cdt + timedelta(days=1)
