from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys

# start_date = sys.argv[1]
# end_date = sys.argv[2]
# loadtype = sys.argv[3]  #create/append

loadtype = 'create'
start_date = '2018-12-01'
end_date = '2018-12-31'

cdt = datetime.strptime(start_date, '%Y-%m-%d').date()
edt = datetime.strptime(end_date, '%Y-%m-%d').date()
spark = SparkSession.builder.appName("incremental").enableHiveSupport().getOrCreate()

mappedData = spark.sql("""Select uc.userid,uc.cookie from mydb1.cookiemap uc inner join mydb1.userlist usr
                            on lower(usr.userid) = lower(uc.userid)""")

mappedData.createOrReplaceTempView("mappedData")

while cdt <= edt:
    data1 = spark.sql("""Select web.* , uc.userid as cco_id from mydb1.web_data web inner join mappedData uc 
                                on uc.cookie = web.uv
                                where web.viewdate ='""" + str(cdt) + """' limit 100""")
    if loadtype.lower() == 'create':
        data1.write.saveAsTable("mydb1.incrementalData", mode='overwrite', format='orc', compression='snappy')
        loadtype = 'created'
    else:
        data1.write.saveAsTable("mydb1.incrementalData", mode='append', format='orc', compression='snappy')

    cdt = cdt + timedelta(days=1)

#spark-submit --master yarn --deploy-mode cluster --driver-memory 9g --num-executors 10 --executor-memory 12g --conf spark.sql.shuffle.partitions=500 --executor-cores 5 --files /opt/hive-site.xml /mydatax1.py
