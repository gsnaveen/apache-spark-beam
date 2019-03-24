from pyspark.sql import SparkSession
from pyspark.sql.functions import col,unix_timestamp, to_date, split,trim,avg,round,when,count,sum,datediff

spark = SparkSession.builder.appName("lending").enableHiveSupport().getOrCreate()
agent = spark.read.csv('./data/Agent Map.csv', header=True,inferSchema=True)
agentLoans = spark.read.csv('./data/Agent Loans.csv', header=True,inferSchema=True)
loansData = spark.read.csv('./data/Loan Data.csv', header=True,inferSchema=True)

agent = agent.withColumn('start_date_dt',
                   to_date(unix_timestamp(col('START_DATE'), 'MM/dd/yyyy').cast("timestamp")))
agent = agent.withColumn('end_date_dt',
                   to_date(unix_timestamp(col('END_DATE'), 'MM/dd/yyyy').cast("timestamp")))

loansData = loansData.withColumn('decision_dt',to_date(unix_timestamp(col('decision_d'),'MM/dd/yyyy').cast('timestamp')))
loansData = loansData.withColumn('last_pymnt_dt',to_date(unix_timestamp(col('last_pymnt_d'),'MM/dd/yyyy').cast('timestamp')))
loansData = loansData.withColumn('next_pymnt_dt',to_date(unix_timestamp(col('next_pymnt_d'),'MM/dd/yyyy').cast('timestamp')))
loansData = loansData.withColumn('term_int',split(trim(loansData.term),' ')[0].cast('integer'))

agentandLoans = agentLoans.join(agent, (agentLoans.AGENT_NUMBER == agent.AGENT_ID),'left')\
                .join(loansData, ((agentLoans.ID == loansData.id) & (loansData.decision_dt.between(agent.start_date_dt, agent.end_date_dt)))) \
                .select(agent.AGENT_ID,agent.NAME,agent.SKILL_LEVEL,agent.TEAM,agent.start_date_dt,agent.end_date_dt,agentLoans.ID.alias('loan_id') \
                    ,loansData.decision_dt ,loansData.term_int,loansData.member_id,loansData.loan_amnt,loansData.grade
                    ,loansData.sub_grade ,loansData.loan_status ,loansData.purpose,loansData.revol_bal
                    ,loansData.revol_util,loansData.last_pymnt_amnt,loansData.last_pymnt_dt,loansData.next_pymnt_dt)\
                    .withColumn('DateDiff',datediff(loansData.decision_dt,agent.start_date_dt))

#Average loan term
#Av
agentandLoans.groupby('NAME').agg(round(avg('term_int'),2).alias('AvgTerm')).show()

#% of ChargeOff loans
agentandLoans.withColumn('Term',agentandLoans.term_int).withColumn('chargedOff',when(agentandLoans.loan_status == 'Charged Off',agentandLoans.loan_id))\
    .groupby('Term').agg(round(count('loan_id')/count('chargedOff'),2).alias('ChargedOff%'),count('loan_id'),count('chargedOff'), count('loan_id')+count('chargedOff')).show()

#daecision date > 60 days in the role
agentandLoans.groupby('NAME').agg(count(when(datediff(loansData.decision_dt,agent.start_date_dt) > 60, agentandLoans.loan_id)).alias('counts') \
                                    , sum(when(datediff(loansData.decision_dt,agent.start_date_dt) > 60, 1)).alias('Sums')).show()

# df.select(expr("length(name)")).collect()
# agent.show()
# agent.printSchema()
# agentLoans.show()
# agentLoans.printSchema()
# loansData.show()
# loansData.printSchema()
# agentandLoans.show()
agentandLoans.printSchema()
