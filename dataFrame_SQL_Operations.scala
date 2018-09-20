#Group by Multiple columns, Alias and creating multiple metrics

df.groupBy(col("viewdate").alias("dim1"),col("country_code")).agg(min("viewdate").alias("Min"),max("viewdate").alias("Max")).show()
