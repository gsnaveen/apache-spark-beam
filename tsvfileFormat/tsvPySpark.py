from pyspark.sql import SparkSession
from pyspark.sql.types import *


# from pyspark.sql.types import StructType, StructField, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("desc", StringType(), True),
    StructField("dated", DateType(), True),
    StructField("dtime", TimestampType(), True)])

spark = SparkSession.builder.appName("loadTsv").enableHiveSupport().getOrCreate()

# indata = spark.read.csv('data.tsv', header=True,inferSchema=True,sep='\t',multiLine=True)

indata = spark.read.csv('data.tsv', header=False,sep='\t',escape="\"",multiLine=True,schema=schema)


indata.show(truncate=False)
indata.printSchema()
print((indata.count(), len(indata.columns)))

indata.show(vertical=True,truncate=False)


"""
self: DataFrameReader, path, 
schema=None,
sep=None,
encoding=None, 
quote=None, 
escape=None, 
comment=None,
header=None, 
inferSchema=None, 
ignorLeadingWhiteSpace=None,
ignoreTrailingWhiteSpace=None, 
nullValue=None, 
nanValue=None,
positivelnf=None, 
negativelnf=None, 
dateFormat=None,
timestampFormat=None, 
maxColumns=None, 
maxCharsPerColumn=None,
maxMalformedLogPerPartition=None, 
mode=None, 
columnNameOfCorruptRecord=None,
multiLine=None, 
charToEscapeQuoteEscaping=None, 
samplingRatio=None,
enforceSchema=None, 
emptyValue=None

# root
#  |-- id: integer (nullable = true)
#  |-- desc: string (nullable = true)
# https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/
# https://stackoverflow.com/questions/44558139/how-to-read-csv-without-header-and-name-them-with-names-while-reading-in-pyspark
"""