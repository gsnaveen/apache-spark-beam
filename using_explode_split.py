from pyspark.sql.functions import translate,split,explode
df = spark.createDataFrame([("ASR 1000, C3850",1),("C2960, C3850",2)],['product_family','row'])
df = df.withColumn("myprod", explode(split(translate(df['product_family']," ",""),",")))
