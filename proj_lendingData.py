from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp, to_date, \
            split,trim,avg,round,when,count,sum,max,datediff,udf,lit
from pyspark.sql.window import Window
from pyspark.sql.types import *

import sys
import re
# runID = sys.argv[1]
# stagingDB = sys.argv[2]
# baseDB = stagingDB + "."+runID +"_"
# startDate = sys.argv[3]
# endDate = sys.argv[4]

spark = SparkSession.builder.appName("lending").enableHiveSupport().getOrCreate()
agent = spark.read.csv('./data/Agent Map.csv', header=True,inferSchema=True)
agentLoans = spark.read.csv('./data/Agent Loans.csv', header=True,inferSchema=True)
loansData = spark.read.csv('./data/Loan Data.csv', header=True,inferSchema=True)

# adding all dates to one data attribute as struct data type
def allDatesFunCall(start_date_dt,end_date_dt,decision_dt,last_pymnt_dt,next_pymnt_dt):
    mystruct = {"start_date_dt" : None
                ,"end_date_dt" : None
                ,"decision_dt" : None
                ,"last_pymnt_dt" : None
                ,"next_pymnt_dt" : None}

    mystruct["start_date_dt"] = start_date_dt
    mystruct["end_date_dt"] = end_date_dt
    mystruct["decision_dt"] = decision_dt
    mystruct["last_pymnt_dt"] = last_pymnt_dt
    mystruct["next_pymnt_dt"] = next_pymnt_dt

    return mystruct

allDatesFunCallWithCol = udf(allDatesFunCall,
                   StructType().add("start_date_dt", DateType(), True)\
                             .add("end_date_dt", DateType(), True, None) \
                             .add("decision_dt", DateType(), True, None) \
                             .add("last_pymnt_dt", DateType(), True, None) \
                             .add("next_pymnt_dt", DateType(), True, None))

#Cleaning the percentage attribute to be float.
def cleanPercent(inValue):
    return float(re.sub('\s+|%', '', inValue))

cleanPercentWithCol = udf(cleanPercent, FloatType())

# Creating a Map attribute
def turnStringToMap(inValue):
    myDict = {}
    inList = inValue.split('&')
    for keyValue in inList:
        keyValueSplit = keyValue.split('=')
        myDict[keyValueSplit[0]] = keyValueSplit[1]
    return myDict

turnStringToMapWithCol = udf(turnStringToMap, MapType(StringType(), StringType()))

strToSplit = "a=aa&b=bb&c=cc"

agent = agent.withColumn('start_date_dt',
                   to_date(unix_timestamp(col('START_DATE'), 'MM/dd/yyyy').cast("timestamp")))
agent = agent.withColumn('end_date_dt',
                   to_date(unix_timestamp(col('END_DATE'), 'MM/dd/yyyy').cast("timestamp")))

loansData = loansData.withColumn('decision_dt',to_date(unix_timestamp(col('decision_d'),'MM/dd/yyyy').cast('timestamp')))
loansData = loansData.withColumn('last_pymnt_dt',to_date(unix_timestamp(col('last_pymnt_d'),'MM/dd/yyyy').cast('timestamp')))
loansData = loansData.withColumn('next_pymnt_dt',to_date(unix_timestamp(col('next_pymnt_d'),'MM/dd/yyyy').cast('timestamp')))
loansData = loansData.withColumn('term_int',split(trim(loansData.term),' ')[0].cast('integer'))

agentandLoans = agentLoans.join(agent, (agentLoans.AGENT_NUMBER == agent.AGENT_ID),how='left')\
                .join(loansData, ((agentLoans.ID == loansData.id) & (loansData.decision_dt.between(agent.start_date_dt, agent.end_date_dt))),how='inner') \
                .select(agent.AGENT_ID,agent.NAME,agent.SKILL_LEVEL,agent.TEAM,agent.start_date_dt,agent.end_date_dt,agentLoans.ID.alias('loan_id') \
                    ,loansData.decision_dt ,loansData.term_int,loansData.int_rate,loansData.member_id,loansData.loan_amnt,loansData.grade
                    ,loansData.sub_grade ,loansData.loan_status ,loansData.purpose,loansData.revol_bal
                    ,loansData.revol_util,loansData.last_pymnt_amnt,loansData.last_pymnt_dt,loansData.next_pymnt_dt)\
                    .withColumn('DateDiff',datediff(loansData.decision_dt,agent.start_date_dt))

agentandLoans = agentandLoans.withColumn("udfallDates", \
                                allDatesFunCallWithCol('start_date_dt','end_date_dt','decision_dt','last_pymnt_dt','next_pymnt_dt'))

agentandLoans = agentandLoans.withColumn("udfint_rate", \
                                            cleanPercentWithCol('int_rate'))

agentandLoans = agentandLoans.withColumn("mapattr", \
                                         turnStringToMapWithCol(lit(strToSplit)))
#Average loan term
agentandLoans.groupby('NAME').agg(round(avg('term_int'),2).alias('AvgTerm')).show()

#% of ChargeOff loans
agentandLoans.withColumn('Term',agentandLoans.term_int).withColumn('chargedOff',when(agentandLoans.loan_status == 'Charged Off',agentandLoans.loan_id))\
    .groupby('Term').agg(round(count('loan_id')/count('chargedOff'),2).alias('ChargedOff%'),count('loan_id'),count('chargedOff'), count('loan_id')+count('chargedOff')).show()

#decision date > 60 days in the role
agentandLoans.groupby('NAME').agg(count(when(datediff(loansData.decision_dt,agent.start_date_dt) > 60, agentandLoans.loan_id)).alias('counts') \
                                    , sum(when(datediff(loansData.decision_dt,agent.start_date_dt) > 60, 1)).alias('Sums')).show()

#Getting max Date for the decision date in the following 2 ways
var = agentandLoans.selectExpr("max(decision_dt)").collect() #for taking care of roll-over year
print(var[0][0])
# varMax = agentandLoans.decision_dt.max().collect()
varMax = agentandLoans.agg(max(agentandLoans.decision_dt)).collect() #head()[0]
print(varMax[0][0])
# df.select(expr("length(name)")).collect()
# agent.show()
# agent.printSchema()
# agentLoans.show()
# agentLoans.printSchema()
# loansData.show()
# loansData.printSchema()
# agentandLoans.show()


repName = 'avgLoanTerm'
agentandLoans.write.saveAsTable(repName + "_rep", mode='overwrite', format='orc' , compression='snappy')
agentandLoans.printSchema()
agentandLoans.show()
# Selecting MAP and STRUCT format attributes in different ways
agentandLoans.select(agentandLoans.mapattr["a"].alias("myCol")
                     ,agentandLoans.mapattr.a.alias("myColDirect")
                     ,agentandLoans.mapattr.z.alias("myColDirectz")
                     ,agentandLoans.udfallDates.start_date_dt.alias("StartDate")).show()
